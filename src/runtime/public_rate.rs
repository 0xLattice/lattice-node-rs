//! Arrival budgets from pinned Swift PublicReadRateLimit.swift. Callers supply
//! monotonic seconds and the observed socket IP, never a forwarded header.
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct RateLimits {
    pub general: f64,
    pub expensive: f64,
    pub listener: f64,
    pub submit: f64,
}
impl Default for RateLimits {
    fn default() -> Self {
        Self {
            general: 25.,
            expensive: 1.,
            listener: 200.,
            submit: 10.,
        }
    }
}
impl RateLimits {
    pub fn validate(self) -> Result<Self, &'static str> {
        if [self.general, self.expensive, self.listener, self.submit]
            .iter()
            .any(|v| !v.is_finite() || *v < 0.)
        {
            return Err("public rates must be finite and nonnegative");
        }
        // Swift traps converting an out-of-range derived ceiling to Int. Refuse
        // the configuration instead; no allocation is made from this ceiling.
        if self.general.max(self.expensive).max(self.listener) * 10. >= isize::MAX as f64
            || self.submit * 2. == f64::INFINITY
        {
            return Err("public rates exceed supported range");
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteClass {
    Exempt,
    General,
    Expensive,
    Submit,
}
impl RouteClass {
    pub fn classify(method: &str, path: &str) -> Self {
        let parts: Vec<_> = path.split('/').filter(|p| !p.is_empty()).collect();
        match parts.as_slice() {
            ["transactions"] if method == "POST" => Self::Submit,
            ["health"] if method == "GET" || method == "HEAD" => Self::Exempt,
            ["api", "block", block] if *block != "latest" => Self::Expensive,
            ["api", "block", _, "transactions" | "children"]
            | ["api", "transaction", _]
            | ["api", "chain", "endpoints"]
            | ["api", "deposits"]
            | ["volumes", _] => Self::Expensive,
            _ => Self::General,
        }
    }
}
#[derive(Clone, Copy)]
struct Bucket {
    capacity: f64,
    rate: f64,
    tokens: f64,
    updated: f64,
}
impl Bucket {
    fn new(rate: f64, bank: f64, now: f64) -> Self {
        let capacity = (rate * bank).max(1.);
        Self {
            capacity,
            rate,
            tokens: capacity,
            updated: now,
        }
    }
    fn take(&mut self, now: f64) -> bool {
        self.tokens = self
            .capacity
            .min(self.tokens + (now - self.updated).max(0.) * self.rate);
        self.updated = now;
        if self.tokens < 1. {
            return false;
        }
        self.tokens -= 1.;
        true
    }
    fn completion(&self) -> f64 {
        if self.rate <= 0. {
            return f64::INFINITY;
        }
        let deficit = self.capacity - self.tokens;
        if deficit > 0. {
            self.updated + deficit / self.rate
        } else {
            self.updated
        }
    }
}
#[derive(Default)]
struct Client {
    general: Option<Bucket>,
    expensive: Option<Bucket>,
}
impl Client {
    fn completion(&self) -> f64 {
        self.general
            .as_ref()
            .map_or(f64::NEG_INFINITY, Bucket::completion)
            .max(
                self.expensive
                    .as_ref()
                    .map_or(f64::NEG_INFINITY, Bucket::completion),
            )
    }
}
pub struct RateLimiter {
    limits: RateLimits,
    ceiling: usize,
    clients: BTreeMap<String, Client>,
    earliest: f64,
    listener: Option<Bucket>,
    submit: Option<Bucket>,
}
impl RateLimiter {
    pub fn new(limits: RateLimits, now: f64) -> Result<Self, &'static str> {
        let limits = limits.validate()?;
        Ok(Self {
            ceiling: (limits.general.max(limits.expensive).max(limits.listener) * 10.)
                .ceil()
                .max(1.) as usize,
            clients: BTreeMap::new(),
            earliest: f64::INFINITY,
            listener: (limits.listener > 0.).then(|| Bucket::new(limits.listener, 1., now)),
            submit: (limits.submit > 0.).then(|| Bucket::new(limits.submit, 2., now)),
            limits,
        })
    }
    pub fn tracked_clients(&self) -> usize {
        self.clients.len()
    }
    pub fn admit(&mut self, client: &str, route: RouteClass, now: f64) -> bool {
        if route == RouteClass::Exempt {
            return true;
        }
        if !self.take_client(client, route, now) {
            return false;
        }
        let bucket = if route == RouteClass::Submit {
            &mut self.submit
        } else {
            &mut self.listener
        };
        bucket.as_mut().is_none_or(|b| b.take(now))
    }
    fn take_client(&mut self, key: &str, route: RouteClass, now: f64) -> bool {
        let expensive = matches!(route, RouteClass::Expensive | RouteClass::Submit);
        let rate = if expensive {
            self.limits.expensive
        } else {
            self.limits.general
        };
        if rate <= 0. {
            return true;
        }
        if !self.clients.contains_key(key) {
            if self.clients.len() >= self.ceiling && now >= self.earliest {
                self.clients.retain(|_, c| c.completion() > now);
                self.earliest = self
                    .clients
                    .values()
                    .map(Client::completion)
                    .fold(f64::INFINITY, f64::min);
            }
            if self.clients.len() >= self.ceiling {
                return true;
            }
        }
        let client = self.clients.entry(key.into()).or_default();
        let bucket = if expensive {
            &mut client.expensive
        } else {
            &mut client.general
        };
        let admitted = bucket
            .get_or_insert_with(|| Bucket::new(rate, if expensive { 10. } else { 2. }, now))
            .take(now);
        self.earliest = self.earliest.min(client.completion());
        admitted
    }
}

use lattice_node::runtime::public_rate::{RateLimiter, RateLimits, RouteClass};
#[test]
fn unchanged_swift_arrival_budgets_and_route_classification() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("public-rate-swift.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let r = case["rates"].as_array().unwrap();
        let mut limiter = RateLimiter::new(
            RateLimits {
                general: r[0].as_f64().unwrap(),
                expensive: r[1].as_f64().unwrap(),
                listener: r[2].as_f64().unwrap(),
                submit: r[3].as_f64().unwrap(),
            },
            0.,
        )
        .unwrap();
        for (event, expected) in case["events"]
            .as_array()
            .unwrap()
            .iter()
            .zip(case["observations"].as_array().unwrap())
        {
            let class = RouteClass::classify(
                event["method"].as_str().unwrap(),
                event["path"].as_str().unwrap(),
            );
            assert_eq!(
                format!("{class:?}").to_lowercase(),
                expected["class"].as_str().unwrap()
            );
            assert_eq!(
                limiter.admit(
                    event["client"].as_str().unwrap(),
                    class,
                    event["time"].as_f64().unwrap()
                ),
                expected["admitted"].as_bool().unwrap(),
                "{event}"
            );
            assert_eq!(
                limiter.tracked_clients(),
                expected["tracked"].as_u64().unwrap() as usize
            );
        }
    }
}
#[test]
fn invalid_operator_rates_return_error() {
    for value in [f64::NAN, f64::INFINITY, -1., f64::MAX] {
        assert!(
            RateLimiter::new(
                RateLimits {
                    general: value,
                    ..Default::default()
                },
                0.
            )
            .is_err()
        );
    }
}

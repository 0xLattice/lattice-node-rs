use super::*;
impl Host {
    /// Cancel abandoned operational reads before draining work or accepting a
    /// replacement command. Consensus jobs are never cancelled by HTTP loss.
    pub(super) async fn cancel_abandoned_fetches(&mut self) -> anyhow::Result<()> {
        let cancelled: Vec<_> = self
            .fetches
            .iter()
            .filter_map(|(id, fetch)| {
                ((matches!(fetch, Fetch::Operator(_))
                    && self.replies.get(id).is_none_or(|reply| reply.is_closed()))
                    || matches!(fetch, Fetch::Worker(reply,cancel) if reply.is_closed() || cancel.load(std::sync::atomic::Ordering::Acquire)))
                .then_some(*id)
            })
            .collect();
        for id in cancelled {
            self.fetches.remove(&id);
            self.replies.remove(&id);
            // FIFO transport commands put cancellation ahead of a new fetch.
            self.network.cancel_fetch(id).await?;
        }
        Ok(())
    }
}

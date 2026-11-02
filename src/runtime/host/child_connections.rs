//! Private issued child execution; live admission remains behind the fact turn.
use super::*;
pub(super) struct Task {
    path: Vec<String>,
    cid: String,
    task: crate::core::levels::ChildExecutionTask,
    session: acquire::Session,
    content: crate::storage::ConnectionContent,
}
pub(super) struct Completion {
    path: Vec<String>,
    cid: String,
    done: crate::core::levels::ChildExecutionCompletion,
    content: crate::storage::ConnectionContent,
}
impl Task {
    pub(super) fn run(self) -> Result<Completion, crate::storage::StoreError> {
        let done = self.task.run(
            &acquire::Source {
                snapshot: self.content.snapshot(),
                session: &self.session,
            },
            now(),
        )?;
        self.session.check()?;
        if let Some(header) = done.header() {
            self.content.preserve_acquired(
                header,
                &self.session.volumes(),
                &self.session.entries(),
            )?;
        }
        Ok(Completion {
            path: self.path,
            cid: self.cid,
            done,
            content: self.content,
        })
    }
}
impl Host {
    pub(super) fn prepare_child_connection(
        &mut self,
        path: Vec<String>,
        job: lattice_block_tree::ConnectJob,
    ) -> anyhow::Result<Option<Task>> {
        let key = (path.clone(), job.block_hash().to_owned());
        if self.active_child_connections.contains(&key) {
            return Ok(None);
        }
        let Some(core) = self.levels.get(&path) else {
            return Ok(None);
        };
        if !core.owns_connection(job.block_hash()) {
            return Ok(None);
        }
        let parent = self.child_parent(&path)?;
        let (task, effects) =
            self.levels
                .get_mut(&path)
                .unwrap()
                .prepare_connection(&job, &parent, now())?;
        self.queue.push_back(Work::Child(path.clone(), effects));
        let Some(task) = task else {
            return Ok(None);
        };
        let id = format!("{}:{}", path.join("/"), job.block_hash());
        let content = crate::storage::ConnectionContent::new(
            self.storage.preflight_snapshot()?,
            self.storage.clone(),
            &id,
            self.connection_pins.remove(&id).unwrap_or_default(),
        );
        self.active_child_connections.insert(key);
        Ok(Some(Task {
            path,
            cid: job.block_hash().into(),
            task,
            content,
            session: acquire::Session::new(
                self.acquisition_requests.clone(),
                self.content_cache.clone(),
            ),
        }))
    }
    pub(super) fn complete_child_connection(&mut self, done: Completion) -> anyhow::Result<()> {
        if !self
            .active_child_connections
            .remove(&(done.path.clone(), done.cid.clone()))
            || self.levels.get(&done.path).is_none()
        {
            return Ok(());
        }
        let storage = self.storage.clone();
        storage.atomic_fact_turn(|| -> anyhow::Result<()> {
            let parent = self.child_parent(&done.path)?;
            let core = self.levels.get_mut(&done.path).unwrap();
            let effects = core.complete_connection(done.done, &parent, now())?;
            if core.tree().is_executed(&done.cid) || core.tree().is_excluded_root(&done.cid) {
                self.needs_fetch.remove(&done.cid);
            }
            self.queue
                .push_back(Work::Child(done.path.clone(), effects));
            self.wake_children()?;
            self.child_proofs(&done.path)?;
            Ok(())
        })?;
        self.completed_connection_content.push(done.content);
        Ok(())
    }
}

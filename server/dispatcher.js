function createDispatcher(store, log = console) {
  function sendJson(ws, payload) {
    ws.send(JSON.stringify(payload));
  }

  function dispatch() {
    while (true) {
      const nextJob = store.dequeueQueuedJob();
      if (!nextJob) {
        return;
      }

      const worker = store.getFirstIdleWorker();
      if (!worker) {
        store.requeueJob(nextJob.jobId, true);
        return;
      }

      if (!worker.ws || worker.ws.readyState !== 1) {
        store.removeWorkerSocket(worker.ws);
        store.requeueJob(nextJob.jobId, true);
        continue;
      }

      const assigned = store.assignJob(nextJob, worker.workerId);
      if (!assigned) {
        store.requeueJob(nextJob.jobId, true);
        continue;
      }

      try {
        sendJson(worker.ws, {
          type: "assign",
          jobId: nextJob.jobId,
          payload: {
            a: nextJob.a,
            b: nextJob.b,
          },
        });
      } catch (error) {
        log.error("Failed to send job assignment:", error.message);
        store.removeWorkerSocket(worker.ws);
        store.requeueJob(nextJob.jobId, true);
      }
    }
  }

  return { dispatch };
}

module.exports = { createDispatcher };

function createDispatcher(store, auth, log = console) {
  function sendJson(ws, payload) {
    ws.send(JSON.stringify(payload));
  }

  function buildWorkerPayload(job) {
    if (!job || !job.task || typeof job.task.op !== "string") {
      throw new Error("Invalid job payload.");
    }
    if (job.task.op === "run_js") {
      if (!auth || typeof auth.signWorkerAssignment !== "function") {
        throw new Error("Missing worker assignment signer.");
      }
      return {
        op: "run_js",
        signedTask: auth.signWorkerAssignment(job),
      };
    }
    return job.task;
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
        const workerPayload = buildWorkerPayload(nextJob);
        sendJson(worker.ws, {
          type: "assign",
          jobId: nextJob.jobId,
          payload: workerPayload,
        });
      } catch (error) {
        log.error("Failed to assign job:", error.message);
        const failAttempt = store.failJob(worker.workerId, nextJob.jobId, `dispatch_error:${error.message}`);
        if (!failAttempt.ok) {
          store.removeWorkerSocket(worker.ws);
          store.requeueJob(nextJob.jobId, true);
        }
      }
    }
  }

  return { dispatch };
}

module.exports = { createDispatcher };

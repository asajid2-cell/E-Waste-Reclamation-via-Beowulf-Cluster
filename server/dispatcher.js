function createDispatcher(store, auth, log = console) {
  const MAX_ASSIGN_BATCH_SIZE = 16;

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

      const worker = store.getFirstIdleWorker(nextJob);
      if (!worker) {
        store.requeueJob(nextJob.jobId, true);
        return;
      }

      if (!worker.ws || worker.ws.readyState !== 1) {
        store.removeWorkerSocket(worker.ws);
        store.requeueJob(nextJob.jobId, true);
        continue;
      }

      const availableSlots = Math.max(1, store.getWorkerAvailableSlots(worker.workerId));
      const desiredBatchSize = Math.max(1, Math.min(MAX_ASSIGN_BATCH_SIZE, availableSlots));
      const jobs = [nextJob];
      for (let i = 1; i < desiredBatchSize; i += 1) {
        const extraJob = store.dequeueQueuedJob();
        if (!extraJob) {
          break;
        }
        jobs.push(extraJob);
      }

      const assignments = [];
      try {
        for (const job of jobs) {
          const assigned = store.assignJob(job, worker.workerId);
          if (!assigned) {
            store.requeueJob(job.jobId, true);
            continue;
          }
          assignments.push({
            jobId: job.jobId,
            payload: buildWorkerPayload(job),
          });
        }

        if (assignments.length === 0) {
          continue;
        }

        if (assignments.length === 1 || !worker.supportsAssignBatch) {
          for (const assignment of assignments) {
            sendJson(worker.ws, {
              type: "assign",
              jobId: assignment.jobId,
              payload: assignment.payload,
            });
          }
        } else {
          sendJson(worker.ws, {
            type: "assign_batch",
            assignments,
          });
        }
      } catch (error) {
        log.error("Failed to assign job:", error.message);
        let sawFailure = false;
        for (const assignment of assignments) {
          const failAttempt = store.failJob(worker.workerId, assignment.jobId, `dispatch_error:${error.message}`);
          sawFailure = sawFailure || !failAttempt.ok;
        }
        if (sawFailure) {
          store.removeWorkerSocket(worker.ws);
          for (const job of jobs) {
            store.requeueJob(job.jobId, true);
          }
        }
      }
    }
  }

  return { dispatch };
}

module.exports = { createDispatcher };

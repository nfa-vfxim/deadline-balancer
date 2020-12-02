import math

from Deadline.Events import *
from Deadline.Scripting import *


def GetDeadlineEventListener():
    return DeadlineBalancer()


def CleanupDeadlineEventListener(deadlinePlugin):
    deadlinePlugin.Cleanup()


class DeadlineBalancer(DeadlineEventListener):
    def __init__(self):
        self.OnJobSubmittedCallback += self.OnJobSubmitted
        self.OnHouseCleaningCallback += self.OnHouseCleaning

    def Cleanup(self):
        del self.OnJobSubmittedCallback
        del self.OnHouseCleaningCallback

    def OnJobSubmitted(self, job):
        self.Balance()

    def OnHouseCleaning(self):
        self.Balance()

    def Balance(self):
        print("Starting balancing of current active jobs")

        # function variables
        totalPriority = 0.0
        skipJobs = []
        reusableWorkers = 0

        # get all active jobs
        jobs = RepositoryUtils.GetJobsInState("Active")
        jobsN = len(jobs)

        print("Total amount of jobs balancing: %s" % (jobsN))

        # get all active workers
        workersUnfiltered = RepositoryUtils.GetSlaveInfoSettings(invalidateCache=True)
        workers = []

        # filter workers for active only
        for worker in workersUnfiltered:
            if worker.Info.SlaveIsActive:
                workers.append(worker)

        workersN = len(workers)

        print("Total amount of active workers for balancing: %s" % (workersN))

        # set the total priority for all jobs found
        for job in jobs:
            # get priority
            priority = job.JobPriority

            # update total priority
            totalPriority += priority

        print("The total priority is: %s" % totalPriority)

        # calculate the machine limit for every job
        for job in jobs:

            # calculate the amount of workers applicable for this job
            percent = 0.0
            percent = float(job.JobPriority) / float(totalPriority)

            # if no slaves are available set the amount to 1 to avoid errors
            if math.ceil(percent * workersN) is 0:
                workers = 1
            else:
                workers = math.ceil(percent * workersN)

            print("Handing %s over to %s" % (workers, job))
            RepositoryUtils.SetMachineLimitMaximum(job.JobId, workers)
            job.MachineLimit = workers

            # check if job has rendering frames that are less than applicable amount of workers
            frames = job.JobRenderingTasks + job.JobQueuedTasks
            if frames < workers:
                skipJobs.append(job)
                reusableWorkers += workers - frames
                workers = frames
                print(
                    "Detected fewer frames than handed workers. Handing %s over to %s"
                    % (workers, job)
                )
                job.MachineLimit = workers
                RepositoryUtils.SetMachineLimitMaximum(job.JobId, workers)

        # redistribute reusable workers
        for job in jobs:
            if job in skipJobs:
                continue

            if reusableWorkers > 0:
                # calculate the new amount
                percent = float(job.JobPriority) / float(totalPriority)
                workers = math.ceil(percent * reusableWorkers)

                print("Handing %s extra workers to %s" % (workers, job))
                RepositoryUtils.SetMachineLimitMaximum(
                    job.JobId, job.MachineLimit + workers
                )

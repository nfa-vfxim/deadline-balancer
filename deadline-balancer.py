import math

from Deadline.Events import *
from Deadline.Scripting import *


def GetDeadlineEventListener():
    return DeadlineBalancer()


def CleanupDeadlineEventListener(deadlinePlugin):
    deadlinePlugin.Cleanup()


class DeadlineBalancer(DeadlineEventListener):
    def __init__(self):
        self.OnJobSubmittedCallback += self.on_job_submitted
        self.OnHouseCleaningCallback += self.on_house_cleaning

    def Cleanup(self):
        del self.OnJobSubmittedCallback
        del self.OnHouseCleaningCallback

    def on_job_submitted(self, job):
        license_limit = job.GetJobPluginInfoKeyValue("LicenseLimit")
        if license_limit:
            RepositoryUtils.SetMachineLimitMaximum(
                job.JobId, int(license_limit)
            )
        self.balance()

    def on_house_cleaning(self):
        """Will perform on house cleaning a balance for
        all jobs.
        """
        self.balance()

    def balance(self):
        """Will calculate for all jobs the amount of machines
        that can render the job depending on the priority.

        For example: 2 jobs are rendering. We have 10 machines.
        Job 1 has a priority of 90, job 2 has a priority of 10.

        Job 1 will get 9 machines, job 2 will get 1 machine.

        When a license limit is active, the job will not get any
        more pc's than the active license limit.
        """
        print("Starting balancing of current active jobs")

        # function variables
        total_priority = 0.0
        skip_jobs = []
        reusable_workers = 0

        # get all active jobs
        jobs = RepositoryUtils.GetJobsInState("Active")
        jobs_amount = len(jobs)

        print("Total amount of jobs balancing: %s" % (jobs_amount))

        # get all active workers
        workers_unfiltered = RepositoryUtils.GetSlaveInfoSettings(
            invalidateCache=True
        )
        workers = []

        # filter workers for active only
        for worker in workers_unfiltered:
            if worker.Info.SlaveIsActive:
                workers.append(worker)

        workers_amount = len(workers)

        print(
            "Total amount of active workers for balancing: %s"
            % (workers_amount)
        )

        # set the total priority for all jobs found
        for job in jobs:
            # get priority
            priority = job.JobPriority

            # update total priority
            total_priority += priority

        print("The total priority is: %s" % total_priority)

        # calculate the machine limit for every job
        for job in jobs:

            # calculate the amount of workers applicable for this job
            percent = 0.0
            percent = float(job.JobPriority) / float(total_priority)

            # if no slaves are available set the amount to 1 to avoid errors
            if math.ceil(percent * workers_amount) is 0:
                workers = 1
            else:
                workers = math.ceil(percent * workers_amount)

            license_limit = job.GetJobPluginInfoKeyValue("LicenseLimit")

            if license_limit:
                license_limit = int(license_limit)
                if workers >= license_limit:
                    workers = license_limit
                    print(
                        "Job %s has a license limit, will limit to %s"
                        % (job, str(license_limit))
                    )

            print("Handing %s over to %s" % (workers, job))
            RepositoryUtils.SetMachineLimitMaximum(job.JobId, workers)
            job.MachineLimit = workers

            # check if job has rendering frames that are less than
            # applicable amount of workers
            frames = job.JobRenderingTasks + job.JobQueuedTasks
            if frames < workers:
                skip_jobs.append(job)
                reusable_workers += workers - frames
                workers = frames
                print(
                    "Detected fewer frames than handed workers. "
                    "Handing %s over to %s" % (workers, job)
                )
                job.MachineLimit = workers
                RepositoryUtils.SetMachineLimitMaximum(job.JobId, workers)

        # redistribute reusable workers
        for job in jobs:
            if job in skip_jobs:
                continue

            if reusable_workers > 0:
                # calculate the new amount
                percent = float(job.JobPriority) / float(total_priority)
                workers = math.ceil(percent * reusable_workers)

                # Detect if job has a machine limit
                license_limit = job.GetJobPluginInfoKeyValue("LicenseLimit")
                if license_limit:
                    license_limit = int(license_limit)
                    new_machine_limit = job.MachineLimit + workers
                    if license_limit >= new_machine_limit:
                        RepositoryUtils.SetMachineLimitMaximum(
                            job.JobId, job.MachineLimit + workers
                        )
                    else:
                        print(
                            "Will not add extra machines because "
                            "of license limit"
                        )

                else:
                    print("Handing %s extra workers to %s" % (workers, job))
                    RepositoryUtils.SetMachineLimitMaximum(
                        job.JobId, job.MachineLimit + workers
                    )

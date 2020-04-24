class JobReadyIndicator:
    def __init__(self):
        pass

class JobReadyList(CompletionIndicator):
    def __init__(self, job_list):
        super().__init__(self)
        self.job_list = job_list

class AllJobsReady(CompletionIndicator):
    def __init__(self):
        super().__init__(self)

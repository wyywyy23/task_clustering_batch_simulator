//
// Created by evan on 8/28/19.
//

#include <Util/PlaceHolderJob.h>
#include <workflow/job/PilotJob.h>
#include <map>
#include <string>

namespace wrench {

    PlaceHolderJob::PlaceHolderJob(PilotJob *pilot_job, std::vector<WorkflowTask *> tasks,
                                   unsigned long start_level, unsigned long end_level) {
        this->pilot_job = pilot_job;
        this->tasks = tasks;
        this->start_level = start_level;
        this->end_level = end_level;
    }

    double PlaceHolderJob::getDuration() {
         std::map<std::string, std::string> service_specific_args = this->pilot_job->getServiceSpecificArguments();
         double duration = std::stod(service_specific_args.find("-t")->second);
         // Duration stored in minutes, convert back to seconds
         return (duration - 1) * 60;
    }
}




//
// Created by Henri Casanova on 3/29/18.
//

#ifndef YOUR_PROJECT_NAME_CLUSTERINGWMS_H
#define YOUR_PROJECT_NAME_CLUSTERINGWMS_H

#include <wrench-dev.h>

using namespace wrench;


    class ClusteringWMS : public WMS {

    public:

        ClusteringWMS(std::string hostname, BatchService *batch_service);
        int main() override;

    private:
        BatchService *batch_service;

    };



#endif //YOUR_PROJECT_NAME_CLUSTERINGWMS_H

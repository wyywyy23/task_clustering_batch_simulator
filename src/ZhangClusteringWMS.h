/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#ifndef YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H
#define YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H


#include <wms/WMS.h>
#include <services/compute/batch/BatchService.h>

namespace wrench {

    class ZhangClusteringWMS : public WMS {

    public:

        ZhangClusteringWMS(std::string hostname, BatchService *batch_service);

    private:

        BatchService *batch_service;

        int main() override;

    };

};


#endif //YOUR_PROJECT_NAME_ZHANGCLUSERINGWMS_H

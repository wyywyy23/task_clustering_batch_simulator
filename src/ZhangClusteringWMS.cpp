/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include "ZhangClusteringWMS.h"

namespace wrench {


    ZhangClusteringWMS::ZhangClusteringWMS(std::string hostname, BatchService *batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustring_wms") {
      this->batch_service = batch_service;
    }

    int ZhangClusteringWMS::main() {


    }

};
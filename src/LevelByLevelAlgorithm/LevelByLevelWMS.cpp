/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <wms/WMS.h>
#include "LevelByLevelWMS.h"

#define EXECUTION_TIME_FUDGE_FACTOR 1.1

namespace wrench {

    LevelByLevelWMS::LevelByLevelWMS(std::string hostname, bool overlap, std::string clustering_spec,
                                     BatchService *batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
      this->overlap = overlap;
      this->batch_service = batch_service;
      this->pending_placeholder_job = nullptr;
      this->clustering_spec = clustering_spec;
    }


    int LevelByLevelWMS::main() {



      return 0;
    }
};
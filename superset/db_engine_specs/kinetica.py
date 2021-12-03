# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import logging

from superset.db_engine_specs.base import BaseEngineSpec, LimitMethod

logger = logging.getLogger(__name__)


class KineticaEngineSpec(BaseEngineSpec):
    engine = "kinetica"
    engine_name = "Kinetica Database"
    sqlalchemy_uri_placeholder = "sa_gpudb://{username}:{password}@{kineticahost}:{" \
                                 "port} "
    limit_method = LimitMethod.WRAP_SQL
    max_column_name_length = 200

    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "DATETIME(MSECS_SINCE_EPOCH({col}) / 1000 * 1000)",
        "PT1M": "DATETIME((MSECS_SINCE_EPOCH({col}) / (1000 * 60)) * (1000 * 60))",
        "PT5M": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), MONTH({col}), DAY({col}), "
                "HOUR({col}), ((MINUTE({col}) / 5) * 5), 0, 0))",
        "PT10M": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), MONTH({col}), DAY({col}), "
                 "HOUR({col}), ((MINUTE({col}) / 10) * 10), 0, 0))",
        "PT15M": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), MONTH({col}), DAY({col}), "
                 "HOUR({col}), ((MINUTE({col}) / 15) * 15), 0, 0))",
        "PT30M": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), MONTH({col}), "
                 "DAY({col}), HOUR({col}), ((MINUTE({col}) / 30) * 30), 0, 0))",
        "PT0.5H": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), MONTH({col}), "
                  "DAY({col}), HOUR({col}), ((MINUTE({col}) / 30) * 30), 0, 0))",
        "PT1H": "DATETIME((MSECS_SINCE_EPOCH({col}) / (1000 * 60 * 60)) * (1000 * 60 "
                "* 60))",
        "P1D": "DATETIME((MSECS_SINCE_EPOCH({col}) / (1000 * 60 * 60 * 24)) * (1000 * "
               "60 * 60 * 24))",
        # This assumes start of week is Sunday
        "P1W": "DATETIME(WEEK_TO_EPOCH_MSECS(YEAR({col}),WEEK({col})))",
        "P1M": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), MONTH({col}), 1, 0, 0, 0, "
               "0))",
        "P3M": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), CASE QUARTER({col}) WHEN "
               " 1 THEN 1 WHEN 2 THEN 4 WHEN 3 THEN 7 ELSE 10 END, 1, 0, 0, 0, 0))",
        "P0.25Y": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), CASE QUARTER({col}) WHEN "
                  " 1 THEN 1 WHEN 2 THEN 4 WHEN 3 THEN 7 ELSE 10 END, 1, 0, 0, 0, 0))",
        "P1Y": "DATETIME(DATE_TO_EPOCH_MSECS(YEAR({col}), 1, 1, 0, 0, 0, 0))",
    }

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "DATETIME({col} * 1000)"

    @classmethod
    def epoch_ms_to_dttm(cls) -> str:
        return "DATETIME({col})"

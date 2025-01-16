# -*- coding: utf-8 -*-
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


try:
    from opentelemetry.metrics import Counter, Histogram, get_meter_provider

    HAS_OPENTELEMETRY_INSTALLED = True
except ImportError:  # pragma: NO COVER
    HAS_OPENTELEMETRY_INSTALLED = False
from typing import List
import re
from google.cloud.spanner_v1.metrics.constants import (
    BUILT_IN_METRICS_METER_NAME,
    SPANNER_SERVICE_NAME,
    METRIC_NAME_GFE_LATENCY,
    METRIC_NAME_GFE_MISSING_HEADER_COUNT
)
from google.cloud.spanner_v1 import __version__

class MetricsGfeTracer():
    _instrument_gfe_latency: Histogram
    _instrument_gfe_missing_header_count: Counter
    enabled = False

    def __init__(self) -> None:
        self._create_gfe_metric_instruments()

    def record_gfe_metrics(self, metadata: List):
        if not self.enabled:
            return

        gfe_headers = [header.value for header in metadata if header.key == 'server-timing' and header.value.startswith("gfe")]

        if len(gfe_headers) == 0:
            self.record_gfe_missing_header_count()
            return

        for gfe_header in gfe_headers:
            match = re.search(r'dur=(\d+)', gfe_header)
            if match:
                duration = int(match.group(1))
                self.record_gfe_latency(duration)


    def record_gfe_latency(self, latency: int) -> None:
        print("real called")
        if not self.enabled:
            return
        self._instrument_gfe_latency.record(amount=latency)

    def record_gfe_missing_header_count(self) -> None:
        if not self.enabled:
            return
        self._instrument_gfe_missing_header_count.add(amount=1)

    def _create_gfe_metric_instruments(self):
        meter_provider = get_meter_provider()
        meter = meter_provider.get_meter(
            name=BUILT_IN_METRICS_METER_NAME, version=__version__
        )
        self._instrument_gfe_latency = meter.create_histogram(
            name=f"{SPANNER_SERVICE_NAME}/{METRIC_NAME_GFE_LATENCY}",
            unit="ms",
            description="GFE Latency."
        )
        self._instrument_gfe_missing_header_count = meter.create_counter(
            name=f"{SPANNER_SERVICE_NAME}/{METRIC_NAME_GFE_MISSING_HEADER_COUNT}",
            unit="1",
            description="GFE missing header count."
            
        ) 
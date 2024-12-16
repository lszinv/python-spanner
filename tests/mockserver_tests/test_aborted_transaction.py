# Copyright 2024 Google LLC All rights reserved.
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

from google.cloud.spanner_v1 import (
    BatchCreateSessionsRequest,
    BeginTransactionRequest,
    CommitRequest,
)
from google.cloud.spanner_v1.testing.mock_spanner import SpannerServicer
from google.cloud.spanner_v1.transaction import Transaction
from tests.mockserver_tests.mock_server_test_base import (
    MockServerTestBase,
    add_error,
    aborted_status,
)


class TestAbortedTransaction(MockServerTestBase):
    def test_run_in_transaction_commit_aborted(self):
        # Add an Aborted error for the Commit method on the mock server.
        add_error(SpannerServicer.Commit.__name__, aborted_status())
        # Run a transaction. The Commit method will return Aborted the first
        # time that the transaction tries to commit. It will then be retried
        # and succeed.
        self.database.run_in_transaction(_insert_mutations)

        # Verify that the transaction was retried.
        requests = self.spanner_service.requests
        self.assertEqual(5, len(requests), msg=requests)
        self.assertTrue(isinstance(requests[0], BatchCreateSessionsRequest))
        self.assertTrue(isinstance(requests[1], BeginTransactionRequest))
        self.assertTrue(isinstance(requests[2], CommitRequest))
        # The transaction is aborted and retried.
        self.assertTrue(isinstance(requests[3], BeginTransactionRequest))
        self.assertTrue(isinstance(requests[4], CommitRequest))


def _insert_mutations(transaction: Transaction):
    transaction.insert("my_table", ["col1", "col2"], ["value1", "value2"])

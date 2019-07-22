"""Test hook for verifying correctness of initial sync."""

import os.path
import random

import bson
import bson.errors
import pymongo.errors

from . import cleanup
from . import interface
from . import jsfile
from ..fixtures import replicaset
from ... import errors


class BackgroundInitialSync(interface.Hook):
    """BackgroundInitialSync class.

    After every test, this hook checks if a background node has finished initial sync and if so,
    validates it, tears it down, and restarts it.

    This test accepts a parameter 'n' that specifies a number of tests after which it will wait for
    replication to finish before validating and restarting the initial sync node.

    This requires the ReplicaSetFixture to be started with 'start_initial_sync_node=True'. If used
    at the same time as CleanEveryN, the 'n' value passed to this hook should be equal to the 'n'
    value for CleanEveryN.
    """

    DEFAULT_N = cleanup.CleanEveryN.DEFAULT_N

    def __init__(self, hook_logger, fixture, n=DEFAULT_N, shell_options=None):
        """Initialize BackgroundInitialSync."""
        if not isinstance(fixture, replicaset.ReplicaSetFixture):
            raise ValueError("`fixture` must be an instance of ReplicaSetFixture, not {}".format(
                fixture.__class__.__name__))

        description = "Background Initial Sync"
        interface.Hook.__init__(self, hook_logger, fixture, description)

        self.n = n  # pylint: disable=invalid-name
        self.tests_run = 0
        self.random_restarts = 0
        self._shell_options = shell_options

    def after_test(self, test, test_report):
        """After test execution."""
        self.tests_run += 1

        hook_test_case = BackgroundInitialSyncTestCase.create_after_test(
            self.logger.test_case_logger, test, self, self._shell_options)
        hook_test_case.configure(self.fixture)
        hook_test_case.run_dynamic_test(test_report)


class BackgroundInitialSyncTestCase(jsfile.DynamicJSTestCase):
    """BackgroundInitialSyncTestCase class."""

    JS_FILENAME = os.path.join("jstests", "hooks", "run_initial_sync_node_validation.js")

    def __init__(  # pylint: disable=too-many-arguments
            self, logger, test_name, description, base_test_name, hook, shell_options=None):
        """Initialize BackgroundInitialSyncTestCase."""
        jsfile.DynamicJSTestCase.__init__(self, logger, test_name, description, base_test_name,
                                          hook, self.JS_FILENAME, shell_options)

    def run_test(self):
        """Execute test hook."""
        sync_node = self.fixture.get_initial_sync_node()
        sync_node_conn = sync_node.mongo_client()

        # If it's been 'n' tests so far, wait for the initial sync node to finish syncing.
        if self._hook.tests_run >= self._hook.n:
            self.logger.info(
                "%d tests have been run against the fixture, waiting for initial sync"
                " node to go into SECONDARY state", self._hook.tests_run)
            self._hook.tests_run = 0

            cmd = bson.SON([("replSetTest", 1), ("waitForMemberState", 2),
                            ("timeoutMillis", 20 * 60 * 1000)])
            sync_node_conn.admin.command(cmd)

        # Check if the initial sync node is in SECONDARY state. If it's been 'n' tests, then it
        # should have waited to be in SECONDARY state and the test should be marked as a failure.
        # Otherwise, we just skip the hook and will check again after the next test.
        try:
            while True:
                # TODO SERVER-40078: The server is reporting invalid
                # dates in its response to the replSetGetStatus
                # command
                try:
                    state = sync_node_conn.admin.command("replSetGetStatus").get("myState")
                    break
                except bson.errors.InvalidBSON:
                    continue

            if state != 2:
                if self._hook.tests_run == 0:
                    msg = "Initial sync node did not catch up after waiting 20 minutes"
                    self.logger.exception("{0} failed: {1}".format(self._hook.description, msg))
                    raise errors.TestFailure(msg)

                self.logger.info(
                    "Initial sync node is in state %d, not state SECONDARY (2)."
                    " Skipping BackgroundInitialSync hook for %s", state, self._base_test_name)

                # If we have not restarted initial sync since the last time we ran the data
                # validation, restart initial sync with a 20% probability.
                if self._hook.random_restarts < 1 and random.random() < 0.2:
                    self.logger.info(
                        "randomly restarting initial sync in the middle of initial sync")
                    self.__restart_init_sync(sync_node)
                    self._hook.random_restarts += 1
                return
        except pymongo.errors.OperationFailure:
            # replSetGetStatus can fail if the node is in STARTUP state. The node will soon go into
            # STARTUP2 state and replSetGetStatus will succeed after the next test.
            self.logger.info(
                "replSetGetStatus call failed in BackgroundInitialSync hook, skipping hook for %s",
                self._base_test_name)
            return

        self._hook.random_restarts = 0

        # Run data validation and dbhash checking.
        self._js_test.run_test()

        self.__restart_init_sync(sync_node)

    # Restarts initial sync by shutting down the node, clearing its data, and restarting it.
    def __restart_init_sync(self, sync_node):
        # Tear down and restart the initial sync node to start initial sync again.
        sync_node.teardown()

        self.logger.info("Starting the initial sync node back up again...")
        sync_node.setup()
        sync_node.await_ready()


class IntermediateInitialSync(interface.Hook):
    """IntermediateInitialSync class.

    This hook accepts a parameter 'n' that specifies a number of tests after which it will start up
    a node to initial sync, wait for replication to finish, and then validate the data.

    This requires the ReplicaSetFixture to be started with 'start_initial_sync_node=True'.
    """

    DEFAULT_N = cleanup.CleanEveryN.DEFAULT_N

    def __init__(self, hook_logger, fixture, n=DEFAULT_N):
        """Initialize IntermediateInitialSync."""
        if not isinstance(fixture, replicaset.ReplicaSetFixture):
            raise ValueError("`fixture` must be an instance of ReplicaSetFixture, not {}".format(
                fixture.__class__.__name__))

        description = "Intermediate Initial Sync"
        interface.Hook.__init__(self, hook_logger, fixture, description)

        self.n = n  # pylint: disable=invalid-name
        self.tests_run = 0

    def _should_run_after_test(self):
        self.tests_run += 1

        # If we have not run 'n' tests yet, skip this hook.
        if self.tests_run < self.n:
            return False

        self.tests_run = 0
        return True

    def after_test(self, test, test_report):
        """After test execution."""
        if not self._should_run_after_test():
            return

        hook_test_case = IntermediateInitialSyncTestCase.create_after_test(
            self.logger.test_case_logger, test, self)
        hook_test_case.configure(self.fixture)
        hook_test_case.run_dynamic_test(test_report)


class IntermediateInitialSyncTestCase(jsfile.DynamicJSTestCase):
    """IntermediateInitialSyncTestCase class."""

    JS_FILENAME = os.path.join("jstests", "hooks", "run_initial_sync_node_validation.js")

    def __init__(  # pylint: disable=too-many-arguments
            self, logger, test_name, description, base_test_name, hook):
        """Initialize IntermediateInitialSyncTestCase."""
        jsfile.DynamicJSTestCase.__init__(self, logger, test_name, description, base_test_name,
                                          hook, self.JS_FILENAME)

    def run_test(self):
        """Execute test hook."""
        sync_node = self.fixture.get_initial_sync_node()
        sync_node_conn = sync_node.mongo_client()

        sync_node.teardown()

        self.logger.info("Starting the initial sync node back up again...")
        sync_node.setup()
        sync_node.await_ready()

        # Do initial sync round.
        self.logger.info("Waiting for initial sync node to go into SECONDARY state")
        cmd = bson.SON([("replSetTest", 1), ("waitForMemberState", 2),
                        ("timeoutMillis", 20 * 60 * 1000)])
        sync_node_conn.admin.command(cmd)

        # Run data validation and dbhash checking.
        self._js_test.run_test()

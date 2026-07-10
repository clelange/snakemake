.. _executors:

======================
Using executor plugins
======================

By default, Snakemake will run all rules locally. For distributed computing, i.e. on a cluster or in the cloud, Snakemake offers an interface for so-called executor plugins (as of version 8.0.0 and onward). 

To set up Snakemake for distributed execution on your compute environment, first navigate to the `Snakemake plugin catalog <https://snakemake.github.io/snakemake-plugin-catalog/>`_. Locate an executor suitable for your cluster/cloud environment and follow its respective installation instructions.

.. admonition:: No fitting executor?
   :class: note

   In this case you may check out the `generic cluster executor plugin <https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/cluster-generic.html>`_ and configure it with your compute environment's submission and status check commands.
   Or - even better - you can use `Snakedeploy <https://snakedeploy.readthedocs.io/en/stable/snakemake_developers/scaffold_snakemake_plugins.html>`_ to set up a base package structure for `creating your own executor plugin <https://snakemake.github.io/snakemake-plugin-catalog/index.html#create-new-plugins>`_. 

Depending on your compute environment you may also need to install a :ref:`storage plugin <storage-support>` (e.g. in order to use S3 for input and output files, or in order to efficiently use a shared network filesystem).

To run a workflow using the executor, use the ``--executor`` parameter:

.. code-block:: console

   $ snakemake --executor <executor name> # e.g. 'slurm', 'htcondor' ...

Or, in case you're also using a storage plugin:

.. code-block:: console

   $ snakemake --executor <executor name> --default-storage-provider <storage provider name>


Executor plugins usually add new command line parameters and resources specific to the compute environment they interact with (for example, the slurm executor plugin allows to set cluster partitions as a resource or from command line). To learn more about these parameters, consult the executor plugins documentation on their respective `catalog page <https://snakemake.github.io/snakemake-plugin-catalog/>`_.  In addition, these parameters will also be displayed when running ``snakemake --help``.

The cluster or cloud specific configuration will entail lots of command line options to be chosen and set - consider combining and persisting them as a :ref:`profile <executing-profiles>`.

Experimental detach/attach
--------------------------

Snakemake can experimentally detach from jobs submitted through a non-local executor via ``--detach`` and later resume control via ``--attach``.
Both options require workflow directory locking and an executor that implements detach and attach hooks while persisting external job IDs in Snakemake's metadata.
Local execution as the primary executor, immediate submission, and ``--ignore-incomplete`` are not supported.

When ``--detach`` is used, Snakemake submits currently runnable non-local jobs and exits without cancelling them.
Runnable local jobs are deferred and can execute after a later ``--attach`` invocation.
If no non-local job is currently runnable, detach fails without creating a detached session.

Use ``--attach`` from the same workflow directory with the same persistence backend, executor, targets, configuration, and relevant executor settings that were used for detach.
Snakemake adopts persisted external jobs, including jobs that completed while detached, and continues scheduling work that had not yet been submitted.
A controller interruption during submission leaves a recoverable ``detaching`` session.
If an attached external job reaches terminal failure, the session is retained as failed for diagnostics; recover the incomplete jobs normally, for example with ``--rerun-incomplete``, before starting another detached run.

Executor plugins must be able to query an external job after the original Snakemake process exits.
The planned ``snakemake-executor-plugin-cluster-generic`` integration therefore requires a configured status command; marker-file-only attachment is not part of the initial implementation.

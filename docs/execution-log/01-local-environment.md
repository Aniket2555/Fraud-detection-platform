# 01 — Local Environment

**Starting state:** a `.venv` existed with only `pandas`, `pyspark`,
`scikit-learn` installed. Every PySpark-based test failed with
`Py4JJavaError: SocketTimeoutException: Accept timed out`. `pytest`
couldn't even collect one test module. Result: 5 passing / 12 failing per
the repo's own `TODO.md`.

**End state:** all local packages installed, PySpark genuinely working,
21/21 relevant unit tests passing.

## What was actually wrong (3 separate issues, all needed together)

1. **Missing `HADOOP_HOME`/`winutils.exe`.** PySpark on Windows needs
   Hadoop's native Windows shims even for purely local (non-HDFS)
   operation.
2. **`PYSPARK_PYTHON` not set correctly.** Without it, PySpark's worker
   subprocesses fall back to whatever `python` resolves to on `PATH` —
   on this machine that was the Windows Store app-execution-alias stub,
   which does nothing and exits, so the JVM's accept() call just times
   out waiting for a worker that never starts. Setting `PYSPARK_PYTHON`
   to the venv's `python.exe` fixes it — but the path **must** use
   Windows-style backslashes (`D:\code file\...\python.exe`), not the
   Git-Bash mangled form (`/d/code file/...`) — Java's `ProcessBuilder`
   splits on whitespace when it doesn't recognize the path format, and
   the project directory has a space in it (`Project-2-Real-time-fraudlent
   detection`).
3. **`pytest` couldn't resolve `producers/transaction_producer/test_
   event_mapper.py`'s bare `import event_mapper`** — that directory has
   an `__init__.py`, so pytest's default import mode walks up to `producers/`
   as the insertion point, not `producers/transaction_producer/` itself.

## Commands run

```powershell
# Install winutils.exe + hadoop.dll (Hadoop 3.3.5 build — close enough to
# PySpark 3.5.4's bundled 3.3.4 for the Windows filesystem shims it needs)
mkdir C:\hadoop\bin
curl -fsSL -o C:\hadoop\bin\winutils.exe "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
curl -fsSL -o C:\hadoop\bin\hadoop.dll "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"

# Set persistent User environment variables (PowerShell)
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$userPath;C:\hadoop\bin", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "D:\code file\Project-2-Real-time-fraudlent detection\.venv\Scripts\python.exe", "User")
[Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "D:\code file\Project-2-Real-time-fraudlent detection\.venv\Scripts\python.exe", "User")
```

```bash
# Install all Python packages (~50 packages: xgboost, torch, mlflow,
# pydeequ, all azure-* SDKs, shap, jsonschema, etc.)
.venv/Scripts/python.exe -m pip install -r requirements.txt
```

`pyproject.toml` fix — added under `[tool.setuptools.package-data]`:
```toml
[tool.pytest.ini_options]
pythonpath = ["producers/transaction_producer"]
```

## Verifying it worked

```bash
export HADOOP_HOME="C:\\hadoop"
export PATH="$PATH:/c/hadoop/bin"
.venv/Scripts/python.exe -m pytest databricks/tests ml/tests producers/transaction_producer/test_event_mapper.py -q
# -> 21 passed
```

Note: because these were set as persistent **User** environment variables,
new terminal sessions pick them up automatically — you don't need to
re-export `HADOOP_HOME`/`PYSPARK_PYTHON` by hand unless you're in a shell
that predates when they were set.

## To reproduce this from scratch

1. Download `winutils.exe` + `hadoop.dll` for a Hadoop 3.3.x build (the
   [cdarlint/winutils](https://github.com/cdarlint/winutils) repo hosts
   prebuilt Windows binaries) into some stable folder, e.g. `C:\hadoop\bin`.
2. Set `HADOOP_HOME` to that folder's parent and add `\bin` to `PATH`.
3. Set `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` to the exact
   `.venv\Scripts\python.exe` path, using backslashes.
4. `pip install -r requirements.txt`.
5. Add the `pythonpath` entry to `pyproject.toml` if it's not already
   there (it is now, as of this session).
6. Run `pytest` as above to confirm.

`tests/chaos/` will still fail locally — those tests hit a live deployed
Azure Function endpoint and are expected to fail until Phase 5 is deployed.

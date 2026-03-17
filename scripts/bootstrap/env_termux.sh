export ETL_REPO_ROOT=/data/data/com.termux/files/home/tirehub-system
export ETL_VAR_ROOT=/data/data/com.termux/files/home/var/tirehub-system
export ETL_DATA_ROOT=/data/data/com.termux/files/home/data/tirehub-system
export ETL_DROP_ROOT=/data/data/com.termux/files/home/drop/tirehub-system
export PYDEPS_ROOT=/data/data/com.termux/files/home/tirehub-system/.pydeps/termux-py312
export PYTHONPATH="$ETL_REPO_ROOT:$PYDEPS_ROOT"
echo "Termux bootstrap env loaded"

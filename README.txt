- Installation Requirements for zktree.py util:
    mkvirtualenv kazoo
    pip install -r requirements.txt


    python zktree.py -h
    python zktree.py destroy zk_host path
    python zktree.py export -out_file myfile zk_host sysname
    python zktree.py export -in_file myfile zk_host sysname

- No installation requirements
    python rabbit_tool.py -H host -u guest -p guest export file
    python rabbit_tool.py -H host -u guest -p guest import file -n sysname

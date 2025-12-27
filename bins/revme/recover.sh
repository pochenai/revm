set -x
set -e
../../target/release/revme baltest -n 1001  -t 32 -b 100 -a  -p -d --pre-recover-sender --skip-7702 --datadir ~/test_nodes/ethereum/execution/reth_full_bak --recover-db

echo "## Start generating sqls by other llms ..."

python src/sources/sql_gen/main.py --model sqlcoderapi --kshot 9 --pool 1  --out_file src/sources/raw.txt --select_type Euclidean_mask --dataset ppl_dev_add_sl.json --sl
sleep 1
python src/sources/post_process.py --file src/sources/raw.txt --llm sqlcoder

python src/sources/sql_gen/main.py --model puyuapi --kshot 9 --pool 1  --out_file src/sources/raw.txt --select_type Euclidean_mask --dataset ppl_dev_add_sl.json --sl
sleep 1
python src/sources/post_process.py --file src/sources/raw.txt --llm puyu

python src/sources/sql_gen/main.py --model codellamaapi --kshot 9 --pool 1  --out_file src/sources/raw.txt --select_type Euclidean_mask --dataset ppl_dev_add_sl.json --sl
sleep 1
python src/sources/post_process.py --file src/sources/raw.txt --llm codellama
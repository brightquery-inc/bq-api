# !/bin/bash

directory="/home/ubuntu/terminal/Backend/Operations/from_s3"
log_base_dir="/home/ubuntu/terminal/Backend/Operations/logs/le"

for file in "$directory"/*; do
    if [ -f "$file" ]; then
        file_name="$(basename "$file")"
        log_dir="$log_base_dir/$file_name.log"
        exec >"$log_dir" 2>&1
        # wc -l "$file"
        echo "Execution started for $(basename "$file") on $(date "+%Y-%m-%d-%H:%M:%S")"
        cd /home/ubuntu/terminal/Backend/Operations/from_s3
        vespa feed --progress=10 --connections=12 "$file"
        # vespa feed --progress=10 "$file"
        sleep 20
        # rm "$file"
        # sleep 5
        echo "successfully uploaded data from $(basename "$file") on $(date "+%Y-%m-%d-%H:%M:%S")"
    fi
done

# 243505713 location


#!/bin/bash
let from=1
for a in {1..10}; do
    for b in {1..10}; do
        let to="$from + 5000"
        echo "Generating users $from to $to"
        python populate_user_profile.py --quiet --from $from --to $to &
        let from=$to
        pids[$p]=$!
    done
    for pid in ${pids[*]}; do
        wait $pid
    done
done


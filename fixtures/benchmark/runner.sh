#!/bin/bash

for i in {1..27}
do
    fab bench:clients=$i
done

fab getmerge

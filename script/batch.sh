#!/bin/bash

bash remove_pool.sh test
bash compile.sh
bash update.sh all
bash start_agents.sh
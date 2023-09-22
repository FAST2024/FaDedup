find ./src/ -name "*.cc" -or -name "*.hh" -or -name "*.c" -or -name "*.h" | xargs wc -l
find ./src/ -name "*.cc" -or -name "*.hh" -or -name "*.c" -or -name "*.h" | xargs cat | wc -l
#!/bin/bash

echo "Benchmarking HINT^m Original vs SIMD"
echo "===================================="

# Ensure the dataset is extracted
if [ ! -f "samples/AARHUS-BOOKS_2013.dat" ]; then
    echo "Extracting dataset..."
    gunzip -k samples/AARHUS-BOOKS_2013.dat.gz
fi

echo ""
echo "--- Running Original hint_m ---"
./query_hint_m.exec -m 10 -o all -q gOVERLAPS -r 5 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry > original_results.txt

echo "Original execution finished. Parsing results..."
grep "Total querying time \[secs\]" original_results.txt
grep "Thro" original_results.txt

echo ""
echo "--- Running SIMD hint_m ---"
./query_hint_m_simd.exec -m 10 -o all -q gOVERLAPS -r 5 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry > simd_results.txt

echo "SIMD execution finished. Parsing results..."
grep "Total querying time \[secs\]" simd_results.txt
grep "Thro" simd_results.txt

echo ""
echo "Done!"

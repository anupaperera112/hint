#!/bin/bash

echo "Benchmarking Base HINT^m Scalar vs SIMD"
echo "========================================"

# Ensure the dataset is extracted
if [ ! -f "samples/AARHUS-BOOKS_2013.dat" ]; then
    echo "Extracting dataset..."
    gunzip -k samples/AARHUS-BOOKS_2013.dat.gz
fi

echo ""
echo "--- Running Base HINT^m Scalar (no optimizations) ---"
./query_hint_m.exec -m 10 -q gOVERLAPS -r 5 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry > base_scalar.txt

echo "Scalar execution finished. Results:"
grep "Total querying time \[secs\]" base_scalar.txt
grep "Throughput" base_scalar.txt

echo ""
echo "--- Running Base HINT^m SIMD (no optimizations) ---"
./query_hint_m_simd.exec -m 10 -q gOVERLAPS -r 5 samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry > base_simd.txt

echo "SIMD execution finished. Results:"
grep "Total querying time \[secs\]" base_simd.txt
grep "Throughput" base_simd.txt

echo ""
echo "--- Performance Comparison ---"
SCALAR_TIME=$(grep "Total querying time" base_scalar.txt | awk '{print $NF}')
SIMD_TIME=$(grep "Total querying time" base_simd.txt | awk '{print $NF}')
SCALAR_THROUGHPUT=$(grep "Throughput" base_scalar.txt | awk '{print $NF}')
SIMD_THROUGHPUT=$(grep "Throughput" base_simd.txt | awk '{print $NF}')

echo "Scalar Time: $SCALAR_TIME secs"
echo "SIMD Time: $SIMD_TIME secs"
echo "Scalar Throughput: $SCALAR_THROUGHPUT queries/sec"
echo "SIMD Throughput: $SIMD_THROUGHPUT queries/sec"

echo ""
echo "Done!"

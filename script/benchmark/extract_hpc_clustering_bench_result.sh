JOBS=(job_20240925_085603 job_20240925_085709)

# Execute a command and print the execution result without the newline
function exe() {
  ret=$("$@")
  echo -n $ret
}

printf "Min Cluster Size, Clustered %%, ARI, AMI\n"
for job in "${JOBS[@]}"; do
  echo "$job"
  for i in {0..4}; do
    echo -n $(cat bench_outputs/${job}/job_${i}/out.log | grep "Minimum cluster size: " | awk '{print $7}'); printf ",\t"
    echo -n $(cat bench_outputs/${job}/job_${i}/out.log | grep % | awk '{print $4}'); printf ",\t"
    echo -n $(cat bench_outputs/${job}/job_${i}/out.log | grep ARI | awk '{print $2}'); printf ",\t"
    echo -n $(cat bench_outputs/${job}/job_${i}/out.log | grep AMI | awk '{print $2}')
    echo ""
  done
done
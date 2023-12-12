for ((i=1; i<=10; i++)); do  
    echo $i
    go test -run 2A -race 
done
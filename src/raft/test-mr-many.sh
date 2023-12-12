for ((i=1; i<=10; i++)); do  
    echo $i
    time go test -run 2D  
done
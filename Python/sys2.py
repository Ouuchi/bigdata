from pyspark import SparkContext

def parse_line(line):
    parts = line.split()
    row_id = int(parts[0])
    column_id = int(parts[1])
    return (row_id, column_id)

# def find_common(t):
def parse_list(line):
    parts = line.split()
    row_id = int(parts[0])
    column_id = 1
    return (row_id, column_id)

if __name__ == "__main__":
    # Initialize Spark context
    # target_user=int(input('please input the target userID:'))
    # k=int(input('please input k: '))
    target_user = 3640
    k=5
    max_iteration=2
    alpha=0.2
    sc = SparkContext("local[5]", "Get Neighbors")

    # Read the matrix from the text file (each line contains 'row_id column_id value')
    matrix_rdd = sc.textFile(r'C:\Users\wyz04\Desktop\big data\facebook\train_test_split_2\facebook_train.txt')
    

    # Parse the RDD to a format ((row_id, column_id), value)
    parsed_matrix_rdd = matrix_rdd.map(parse_line)
    

    # for each user, collect his/her friends as a list
    node_neighbors_rdd = parsed_matrix_rdd.groupByKey().mapValues(list)

    ranks=node_neighbors_rdd.map(lambda x:(x[0],1 if x[0]==target_user else 0))

    for iteration in range(max_iteration):
        contributions=node_neighbors_rdd.join(ranks).flatMap(
            lambda x:[(neighbor,x[1][1]/len(x[1][0])) for neighbor in x[1][0]]
        )

        ranks=contributions.reduceByKey(lambda x,y:x+y).mapValues(
            lambda rank: rank*alpha+(1-alpha) if target_user else rank*alpha
        )

    final_ranks=ranks.sortBy(lambda x:x[1],ascending=False).collect()




    
    
    
    # get the list of friends of the given user
    # neighbors_rdd = node_neighbors_rdd.filter(lambda x: x[0] == target_user).map(lambda x: x[1])
    # nn=node_neighbors_rdd.filter(lambda x: x[0]==target_user).map(lambda x:x[1]).collect()

    
    result=final_ranks
    with open('result.txt', 'w') as f:
        for line in result:
            f.write(f"{line}\n")

    # Stop the Spark context
    


    sc.stop()

    # print('*'*30)
    # print(node_neighbors_rdd)
    # print(recommend_friend)
    # print(len(neighbor_neighbor))
    # print(len(neighbor_neighbor_set))
    # print('='*30)

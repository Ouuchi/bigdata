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
    sc = SparkContext("local[5]", "Get Neighbors")

    # Read the matrix from the text file (each line contains 'row_id column_id value')
    matrix_rdd = sc.textFile(r'C:\Users\wyz04\Desktop\big data\facebook\train_test_split_1\facebook_train.txt')
    

    # Parse the RDD to a format ((row_id, column_id), value)
    parsed_matrix_rdd = matrix_rdd.map(parse_line)
    

    # for each user, collect his/her friends as a list
    node_neighbors_rdd = parsed_matrix_rdd.groupByKey().mapValues(list)


    target_user = 1183
    k=5
    neighbor_neighbor_set=[]
    
    # get the list of friends of the given user
    # neighbors_rdd = node_neighbors_rdd.filter(lambda x: x[0] == target_user).map(lambda x: x[1])
    # nn=node_neighbors_rdd.filter(lambda x: x[0]==target_user).map(lambda x:x[1]).collect()

    target_neighbors = node_neighbors_rdd.filter(lambda x: x[0]==target_user).map(lambda x: x[1]).collect()[0]
   
    
    for j in target_neighbors:
        neighbor_neighbor=node_neighbors_rdd.filter(lambda x: x[0] ==j).map(lambda x:x[1]).collect()
        if len(neighbor_neighbor)!=0:
            neighbor_neighbor_set+=neighbor_neighbor[0]
        
    clean_neighbor=[x for x in neighbor_neighbor_set if x not in target_neighbors]
    result=clean_neighbor

    with open('result.txt', 'w') as f:
        for line in result:
            f.write(f"{line}\n")

    # Stop the Spark context
    matrix_rdd = sc.textFile('result.txt')
    node_rdd = matrix_rdd.map(parse_list)
    node_neighbors_rdd = node_rdd.reduceByKey(lambda a,b:a+b)
    node_neighbors_rdd=node_neighbors_rdd.map(lambda x:(x[1],x[0])).sortByKey(False).collect()
    recommend_friend=[]
    for i in range(k):
        recommend_friend.append(node_neighbors_rdd[i][1])
    with open('node_count.txt', 'w') as f:
        for line in node_neighbors_rdd:
            f.write(f"{line}\n")


    sc.stop()

    # print('*'*30)
    # print(node_neighbors_rdd)
    # print(recommend_friend)
    # print(len(neighbor_neighbor))
    # print(len(neighbor_neighbor_set))
    # print('='*30)

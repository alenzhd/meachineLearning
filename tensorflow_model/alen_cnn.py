import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

#number 1 to 10 data
minst = input_data.read_data_sets('MNIST_data',one_hot=True)

def add_layer(inputs,in_size,out_size,activation_function=None,):
    # add one more layer and return the output of this layer
    Weight = tf.Variable(tf.random_normal([in_size,out_size]))
    biases = tf.Variable(tf.zeros([1,out_size])+0.1)
    Wx_plus_b = tf.matmul(inputs,Weight)+biases
    if activation_function is None:
        outputs = Wx_plus_b
    else:
        outputs = activation_function(Wx_plus_b)
    return outputs

def compute_accuracy(v_xs,v_ys):
    global prediction
    # y_ps = sess.run(prediction,)
    y_pre = sess.run(prediction,feed_dict={xs:v_xs})
    correct_prediction = tf.equal(tf.arg_max(y_pre,1),tf.arg_max(v_ys,1))
    accuracy = tf.reduce_mean(tf.cast(correct_prediction,tf.float32))
    result = sess.run(accuracy,feed_dict={xs:v_xs,ys:v_ys})
    return result
#defin placeholeder for inputs to network
xs = tf.placeholder(tf.float32,[None,784])
ys = tf.placeholder(tf.float32,[None,10])
#add output layer
prediction = add_layer(xs,784,10,activation_function=tf.nn.softmax)
# the loss between prediction and real data
cross_entrope = tf.reduce_mean(-tf.reduce_sum(ys*tf.log(prediction)))

train_step = tf.train.GradientDescentOptimizer(0.01).minimize(cross_entrope)

# init variables
init = tf.global_variables_initializer()
# sess = tf.Session()
# sess.run(init)
with tf.Session() as sess:
    sess.run(init)
    for i in range(2000): #90.15%
       # if i%20 == 0:
       batch_xs,batch_ys = minst.train.next_batch(100)
       sess.run(train_step,feed_dict={xs:batch_xs,ys:batch_ys})
       if i%50 == 0:
           print(i)
           print(compute_accuracy(minst.test.images,minst.test.labels))
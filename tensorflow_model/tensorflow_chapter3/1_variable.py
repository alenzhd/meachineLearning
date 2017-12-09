import tensorflow as tf

# create two variables
Weights = tf.Variable(tf.random_normal([784,200]),name="biases")
biases = tf.Variable(tf.zeros(200),name="biases")

W_2 = tf.Variable(Weights.initialized_value(),name="W_2")
W_twice = tf.Variable(Weights.initialized_value()*2,name="W_twice")
# add one op to initialize all variables
# init = tf.global_variables_initializer()

# add one op to save or restore variables
saver = tf.train.Saver()
# later , launch all model
with tf.Session() as sess:
    # sess.run(init)
    # print(sess.run(W_twice),sess.run(W_2))
    # save_path = saver.save(sess,save_path="/data/model.ckpt")
    save_path = saver.restore(sess,save_path="/data/model.ckpt")
    print ("Model saved in file: ", save_path)
    print(sess.run(Weights))
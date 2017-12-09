import tensorflow as tf

sess = tf.Session()
mergad_summary_op = tf.summary.merge_all()
summary_write = tf.summary.FileWriter("/tmp/minst.logs",sess.graph)
total_step = 0
while training:
  total_step += 1
  session.run(training_op)
  if total_step % 100 == 0:
    summary_str = session.run(merged_summary_op)
    summary_writer.add_summary(summary_str, total_step)
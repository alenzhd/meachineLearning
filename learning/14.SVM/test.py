import  numpy as np
print("np.eye(2)=",np.eye(2))
sigmas = [np.eye(2), 2*np.eye(2), np.diag((1,2)), np.array(((2,1),(1,2)))]
# for i in len(sigmas):
print("2*np.eye(2)",2*np.eye(2))
print("np.diag((12))=",np.diag((1,2)))
print("np.array(((2,1),(1,2)))=",np.array(((2,1),(1,2))))
print("sigma=",sigmas)
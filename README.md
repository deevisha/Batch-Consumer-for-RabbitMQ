# Batch-Consumer-for-RabbitMQ

#### I have worked on writing the script for batch consumer which consumes certain batch of messges in the specified time.
#### It satisfies the conditions as follows:
#### i) if the time expires and the messages are less than the specified batch  it sends them to the worker function and does not wait till the queue gets full 
#### ii)If the batch gets full before the specified time,it send the data immedialtely to the worker.

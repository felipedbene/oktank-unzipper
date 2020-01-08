


def stop_ec2():
    

    conn = boto.ec2.connect_to_region("ap-south-1") # or your region
    # Get the current instance's id
    my_id = boto.utils.get_instance_metadata()['instance-id']
    logger.info(' stopping EC2 :'+str(my_id))
    conn.stop_instances(instance_ids=[my_id])
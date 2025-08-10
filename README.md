# ResQFlow
This project implements a distributed task queue system for efficiently managing emergency calls for medical, fire, and police services around Bangalore. Using Apache Kafka as the message broker, the system reliably distributes emergency tasks in real time across multiple workers. Each task includes details like location, priority, severity, or threat level, depending on the type of emergency. The client generates random emergency calls and sends them to Kafka, where workers process them. Each worker calculates the estimated time to reach the location based on preset coordinates (e.g., for hospitals, fire stations, and police stations). For fire emergencies, it calculates an evacuation radius based on priority, for medical emergencies, it assesses urgency from severity, and for police emergencies, it evaluates threat level. A heartbeat mechanism tracks worker status (idle or processing)
<img width="886" height="672" alt="image" src="https://github.com/user-attachments/assets/8de569ad-c642-429c-b4a0-af4583c8e0b7" />

Technology Used: 
Apache Kafka ,
Redis 


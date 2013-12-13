#include <slack/std.h>
#include <slack/map.h>
#include <slack/list.h>
#include <pthread.h>
#include "protocols.h"
#include "packetcore.h"
#include "message.h"
#include "grouter.h"

// WCWeightedFairScheduler: is one part of the W+FQ scheduler.
// It picks the appropriate job from the system of queues.
// If no job in packet core.. the thread waits.
// If there is job, select a Queue and then the job at the head of the Queue
// for running. Update the algorithm parameters and store them back on the queue
// datastructure: start, finish times for the queue and virtual time for the system.

// TODO: Debug this function..

extern router_config rconfig;

/*void *weightedFairScheduler(void *pc)
{
	pktcore_t *pcore = (pktcore_t *)pc;
	List *keylst;
	simplequeue_t *nxtq, *thisq;
	char *nxtkey, *savekey;
	double minftime, minstime, tweight;
	int pktsize, npktsize;
	gpacket_t *in_pkt, *nxt_pkt;
	minftime = -1;

	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);       // die as soon as cancelled
	while (1)
	{
		savekey = NULL;

		verbose(2, "[weightedFairScheduler]:: Worst-case WFQ scheduler processing...");

		pthread_mutex_lock(&(pcore->qlock));
		if (pcore->packetcnt == 0){
			pthread_cond_wait(&(pcore->schwaiting), &(pcore->qlock));
		}
		pthread_mutex_unlock(&(pcore->qlock));
		pthread_testcancel();
		keylst = map_keys(pcore->queues);
		while (list_has_next(keylst) == 1)
		{
//			savekey = NULL;
			nxtkey = list_next(keylst);
			verbose(2, "Looking at queue %s", nxtkey);
			nxtq = map_get(pcore->queues, nxtkey);

			verbose(2, "Size:  %d", nxtq->cursize);

			if (nxtq->cursize == 0)
			{
				verbose(2, "Is empty.");
				continue;
			}

			verbose(2, "Checking if %s->stime=%f <= pcore->vclock=%f && %s->ftime=%f < minftime=%f", nxtkey, nxtq->stime, pcore->vclock, nxtkey, nxtq->ftime, minftime);
			if ((nxtq->stime <= pcore->vclock || pcore -> vclock == -1) && (nxtq->ftime < minftime || minftime == -1))
			{
				verbose(2, "entered minftime if\n");
				savekey = nxtkey;
				//verbose(2, "minftime was %f, setting it to %f\n", minftime, nxtq->ftime);
				//minftime = nxtq->ftime;
			}
		}

		//if (nxtq->cursize == 0) minftime = -1;
		list_release(keylst);
		// if savekey is NULL then release the lock..
		if (savekey == NULL)
		{
			minftime = -1;
			pcore -> vclock = -1;
			continue;
		}
		else
		{
//			verbose(1, "Scheduler looking at queue %s", savekey);
			thisq = map_get(pcore->queues, savekey);
			int status = readQueue(thisq, (void **)&in_pkt, &pktsize);
			if (status == EXIT_SUCCESS)
			{
				verbose(2, "picking from %s\n", savekey);
				writeQueue(pcore->workQ, in_pkt, pktsize);			
				pthread_mutex_lock(&(pcore->qlock));
				pcore->packetcnt--;
				pthread_mutex_unlock(&(pcore->qlock));
			}
			peekQueue(thisq, (void **)&nxt_pkt, &npktsize);
			if (npktsize)
			{
				//thisq->stime = thisq->ftime;
				double temp = thisq->ftime;
				verbose(2,"processor: thisq->ftime was %f\n", thisq->ftime); 
				thisq->ftime = thisq->ftime + ((double)npktsize)/thisq->weight;
				verbose(2,"processor: thisq->ftime is now  %f\n", thisq->ftime);
				thisq->stime = temp;
			}
			minstime = thisq->stime;
			tweight = 0.0;
			keylst = map_keys(pcore->queues);
			while (list_has_next(keylst) == 1)
			{
				nxtkey = list_next(keylst);
				nxtq = map_get(pcore->queues, nxtkey);
				tweight += nxtq->weight;
				if ((nxtq->cursize > 0) && (nxtq->stime < minstime))
					minstime = nxtq->stime;
				if (nxtq->ftime  > minftime)
					minftime = nxtq->ftime;
			}
			list_release(keylst);
			pcore->vclock = max(minstime, (pcore->vclock + ((double)pktsize)/tweight));
		}
	}
}*/


void *weightedFairScheduler(void *pc)
{
	pktcore_t *pcore = (pktcore_t *)pc;
	List *keylst;
	simplequeue_t *nxtq, *thisq;
	char *nxtkey, *savekey;
	double tweight;
	int pktsize, npktsize;
	gpacket_t *in_pkt, *nxt_pkt;

	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);       // die as soon as cancelled
	while (1)
	{
		savekey = NULL;

		verbose(2, "[weightedFairScheduler]:: Worst-case WFQ scheduler processing...");

		pthread_mutex_lock(&(pcore->qlock));
		if (pcore->packetcnt == 0)
		{
			pthread_cond_wait(&(pcore->schwaiting), &(pcore->qlock));
		}
		pthread_mutex_unlock(&(pcore->qlock));

		pthread_testcancel();
		
		int totalWeights = 0;
		keylst = map_keys(pcore->queues);
		while (list_has_next(keylst) == 1)
		{
			nxtkey = list_next(keylst);
			nxtq = map_get(pcore->queues, nxtkey);
			totalWeights += nxtq->weight;
		}
		list_release(keylst);

		keylst = map_keys(pcore->queues);
		while (list_has_next(keylst) == 1)
		{
			nxtkey = list_next(keylst);
			verbose(2, "Looking at queue %s.", nxtkey);
			nxtq = map_get(pcore->queues, nxtkey);

			if (nxtq->cursize == 0)
			{
				verbose(2, "Is empty.");
				continue;
			}
			
			if (nxtq->weightAchieved < nxtq->weight)
			{
				savekey = nxtkey;
				break;
			}
		}
		list_release(keylst);

		if (savekey == NULL)
		{
			continue;
		}
		thisq = map_get(pcore->queues, savekey);

		int status = readQueue(thisq, (void **)&in_pkt, &pktsize);
		writeQueue(pcore->workQ, in_pkt, pktsize);			
		pthread_mutex_lock(&(pcore->qlock));
		pcore->packetcnt--;
		pthread_mutex_unlock(&(pcore->qlock));
	
		pcore->vclock += 1/pktsize;
		thisq->weightAchieved += 1/pktsize; // fix for weight
		if (pcore->vclock >= totalWeights)
		{
			pcore->vclock = 0;
			while (list_has_next(keylst) == 1)
			{
				nxtkey = list_next(keylst);
				nxtq = map_get(pcore->queues, nxtkey);
				nxtq->weightAchieved = 0;
			}
			list_release(keylst);
		}
	}
} 


// WCWeightFairQueuer: function called by the classifier to enqueue
// the packets.. 
// TODO: Debug this function...
int weightedFairQueuer(pktcore_t *pcore, gpacket_t *in_pkt, int pktsize)
{
	char *qkey = tagPacket(pcore, in_pkt);
	simplequeue_t *thisq, *nxtq;
	double minftime, minstime, tweight;
	List *keylst;
	char *nxtkey, *savekey;

	verbose(2, "[weightedFairQueuer]:: Worst-case weighted fair queuing scheduler processing packet w/key %s", qkey);

	pthread_mutex_lock(&(pcore->qlock));

	thisq = map_get(pcore->queues, qkey);
	if (thisq == NULL)
	{
		fatal("[weightedFairQueuer]:: Invalid %s key presented for queue addition", qkey);
		pthread_mutex_unlock(&(pcore->qlock));
		free(in_pkt);
		return EXIT_FAILURE;             // packet dropped..
	}

//	printf("Checking the queue size \n");
	if (thisq->cursize == 0)
	{
		verbose(2, "[weightedFairQueuer]:: inserting the first element.. ");
		double temp = thisq->ftime;
		//thisq->stime = max(pcore->vclock, thisq->ftime);
		thisq->ftime = thisq->ftime + ((double)pktsize)/thisq->weight;
		thisq->stime = max(pcore->vclock, temp);
		verbose(2, "queue: thisq->ftime was %f but is now %f\n", temp, thisq->ftime);
		minstime = thisq->stime;

		keylst = map_keys(pcore->queues);

		while (list_has_next(keylst) == 1)
		{
			nxtkey = list_next(keylst);

			nxtq = map_get(pcore->queues, nxtkey);

			if ((nxtq->cursize > 0) && (nxtq->stime < minstime))
				minstime = nxtq->stime;
		}
		list_release(keylst);

		pcore->vclock = max(minstime, pcore->vclock);
		// insert the packet... and increment variables..
	//	writeQueue(thisq, in_pkt, pktsize);
	//	pcore->packetcnt++;

		// wake up scheduler if it was waiting..
		pcore->packetcnt++;
		if (pcore->packetcnt == 1)
			pthread_cond_signal(&(pcore->schwaiting)); // wake up scheduler if it was waiting..
		pthread_mutex_unlock(&(pcore->qlock));
		verbose(2, "[weightedfairqueuer]:: Adding packet.. ");
		writeQueue(thisq, in_pkt, pktsize);


	//	if (pcore->packetcnt == 1)
	//		pthread_cond_signal(&(pcore->schwaiting));
	//	pthread_mutex_unlock(&(pcore->qlock));
		return EXIT_SUCCESS;
	} else if (thisq->cursize < thisq->maxsize)
	{
		// insert packet and setup variables..
		writeQueue(thisq, in_pkt, pktsize);
		verbose(2, "[weightedfairqueuer]:: Adding packet.. ");
		pcore->packetcnt++;
		pthread_mutex_unlock(&(pcore->qlock));
		return EXIT_SUCCESS;
	} else {
		verbose(1, "[weightedFairQueuer]:: Packet dropped.. Queue for %s is full ", qkey);
		pthread_mutex_unlock(&(pcore->qlock));
		free(in_pkt);
		return EXIT_SUCCESS;
	}
}

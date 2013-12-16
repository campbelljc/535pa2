#include <slack/std.h>
#include <slack/map.h>
#include <slack/list.h>
#include <float.h>
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

void *weightedFairScheduler(void *pc)
{
	pktcore_t *pcore = (pktcore_t *)pc;
	List *keylst;
	simplequeue_t *nxtq, *thisq;
	char *nxtkey, *savekey;
	double tweight = 1;
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

			if (nxtq->weightAchieved < tweight) 
			{
				savekey = nxtkey;
				break;
			}
		}
		list_release(keylst);

		if (savekey == NULL)
		{
			printf("savekey == null for some reason, resetting\n");
			//if theres no queue to select, restart everything
			tweight = 1;
			keylst = map_keys(pcore->queues);
        	while (list_has_next(keylst) == 1)
        	{
        	      nxtkey = list_next(keylst);
        	      nxtq = map_get(pcore->queues, nxtkey);
        	      nxtq->weightAchieved = 0;
        	}
        	list_release(keylst);
			continue;
		}
		thisq = map_get(pcore->queues, savekey);

		int status = readQueue(thisq, (void **)&in_pkt, &pktsize);
		if (status == EXIT_SUCCESS)
		{
			printf("picking from queue %s\n", savekey);
			printf("thisq->weightAchieved = %f , totalWeights = %f\n", thisq->weightAchieved, tweight);
			writeQueue(pcore->workQ, in_pkt, pktsize);
			pthread_mutex_lock(&(pcore->qlock));
			pcore->packetcnt--;
			pthread_mutex_unlock(&(pcore->qlock));
			//pcore->vclock += 1/pktsize;
			thisq->weightAchieved += ((double)pktsize)/thisq->weight;
		}

		if (thisq->weightAchieved > tweight){
			tweight = thisq->weightAchieved;
		}

		if (thisq->weightAchieved == DBL_MAX || tweight == DBL_MAX)
		{ //if anything is maxed out, restart the whole thing
            tweight = 1;
            keylst = map_keys(pcore->queues);
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


// WCWeightFairQueuer: function called by the classifier to enqueue the packets.. 
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

		keylst = map_keys(pcore->queues);

		while (list_has_next(keylst) == 1)
		{
			nxtkey = list_next(keylst);

			nxtq = map_get(pcore->queues, nxtkey);
		}
		list_release(keylst);

		// insert the packet... and increment variables..
		// wake up scheduler if it was waiting..
		pcore->packetcnt++;
		if (pcore->packetcnt == 1)
			pthread_cond_signal(&(pcore->schwaiting)); // wake up scheduler if it was waiting..
		pthread_mutex_unlock(&(pcore->qlock));
		verbose(2, "[weightedfairqueuer]:: Adding packet.. ");
		writeQueue(thisq, in_pkt, pktsize);
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

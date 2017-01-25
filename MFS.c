#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <sys/time.h>

typedef struct _flow {
  int id;
  float arrival_time;
  float transmission_time;
  int priority;
  int line_number;
  bool on_pipe;

  struct _flow *next;
} flow;

#define ID_ATTRIBUTE 0
#define ARRIVAL_TIME_ATTRIBUTE 1
#define TRANSMISSION_TIME_ATTRIBUTE 2
#define PRIORITY_ATTRIBUTE 3
#define TOTAL_NUMBER_OF_ATTRIBUTES 4
#define RATIO_OF_SECONDS_TO_MICROSECONDS 100000

#define BEGINNING_OF_QUEUE 5
#define MIDDLE_OF_QUEUE 6

// Function defintions
void read_file(FILE *input_file);
void run_threads(int number_of_flows);
void *thread_function();
void arrive(flow* flow_to_arrive);
void transmit(flow* flow_to_transmit);
double get_elapsed_time();
void request_pipe(flow *flow_to_execute);
void release_pipe(flow *flow_to_execute);
void dequeue();
void add_to_flow_list(flow *flow_to_add);
void add_to_empty_list(flow *flow_to_add);
void add_to_non_empty_list(flow *flow_to_add);
flow* get_end_of_list();
flow* initialize_node(flow *flow_to_copy);
void add_to_flow_queue(flow *flow_to_add);
void add_to_empty_queue(flow *flow_to_add);
void add_to_non_empty_queue(flow *flow_to_add);
int position_of_flow(flow *current_flow, flow *previous_flow);
bool is_smaller_priority(flow* flow_to_add, flow *current_flow);
void add_to_front_of_queue(flow* flow_to_add, flow *current_flow);
void add_to_mid_of_queue(flow* flow_to_add, flow *current_flow, flow *previous_flow);
void add_to_end_of_queue(flow* flow_to_add, flow *current_flow);
void copy_flows(flow* source, flow* destination);
bool is_empty(flow* root);

// Global variable definitions
struct timeval start;
flow *flows_root = NULL;
flow *queue_root = NULL;
pthread_mutex_t execute_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t execute_condvar = PTHREAD_COND_INITIALIZER;

int main(int argc, char *argv[]) {

  // Set timer when program starts to calculate elapsed time later on
  if (gettimeofday(&start, NULL) == -1)
    printf("Error getting time");

  if (argc != 2) {
    perror("Provide one argument of file name");
    return -1;
  }

  // Variable declarations
  FILE* input_file = fopen(argv[1], "r");
  flows_root = initialize_node(NULL);
  queue_root = initialize_node(NULL);

  if (input_file != NULL)
    read_file(input_file);
  else
    printf("Can't find file");

}

void read_file(FILE *input_file) {
  char line_read[60];
  char *flow_attribute;
  int line_number = 0;
  int attribute_counter = 0;
  int number_of_flows;
  flow *new_flow;

  // Read all the lines in the input file and put them in the appropriate property of the new_flow struct
  while (fgets(line_read, 60, input_file) != NULL) {
    flow_attribute = strtok(line_read, ":\n");

    while (flow_attribute) {

      switch (line_number) {
        case 0:   number_of_flows = atoi(flow_attribute);
                  new_flow = initialize_node(NULL);
                  new_flow->on_pipe = 0;
                  break;

        default:  switch (attribute_counter) {
                    case ID_ATTRIBUTE:                new_flow->id = atoi(flow_attribute);
                                                      break;
                    case ARRIVAL_TIME_ATTRIBUTE:      new_flow->arrival_time = atof(flow_attribute) / 10;
                                                      break;
                    case TRANSMISSION_TIME_ATTRIBUTE: new_flow->transmission_time = atof(flow_attribute) / 10;
                                                      break;
                    case PRIORITY_ATTRIBUTE:          new_flow->priority = atoi(flow_attribute);
                                                      new_flow->line_number = line_number;
                                                      add_to_flow_list(new_flow);
                  }
      }

      flow_attribute = strtok(NULL, ",\n");

      if (line_number > 0) {
        attribute_counter = (attribute_counter + 1) % TOTAL_NUMBER_OF_ATTRIBUTES;
      }
    }

    line_number++;

  }

  free(new_flow);
  fclose(input_file);

  run_threads(number_of_flows);
}

void run_threads(int number_of_flows) {
  pthread_t thread_id[number_of_flows];
  int i = 0;
  flow *current_flow;

  // Create a thread for every flow in flows_root
  for (current_flow = flows_root; current_flow->priority != 0; current_flow = current_flow->next) {
    if (pthread_create(&thread_id[i], NULL, thread_function, current_flow) != 0) {
      printf("Error creating thread");
      return;
    }
    i++;
  }

  for (i = 0; i < number_of_flows; i++) {
    if (pthread_join(thread_id[i], NULL) != 0)
      printf("Error joining thread");
  }

  if (pthread_mutex_destroy(&execute_mutex) != 0)
    printf("Error destroying mutex");
  if (pthread_cond_destroy(&execute_condvar) != 0)
    printf("Error destroying condition variable");
}

void *thread_function(flow* flow_to_execute) {

  arrive(flow_to_execute);

  request_pipe(flow_to_execute);
  transmit(flow_to_execute);
  release_pipe(flow_to_execute);

  pthread_exit(NULL);
}

void arrive(flow* flow_to_arrive) {

  usleep(flow_to_arrive->arrival_time * RATIO_OF_SECONDS_TO_MICROSECONDS);
  printf("Flow %2d arrives: arrival time (%.2f), transmission time (%.1f), priority (%2d). \n", flow_to_arrive->id, flow_to_arrive->arrival_time, flow_to_arrive->transmission_time, flow_to_arrive->priority);
}

void transmit(flow* flow_to_transmit) {
  printf("Flow %2d starts its transmission at time %.2f.\n", flow_to_transmit->id, get_elapsed_time());
  usleep(flow_to_transmit->transmission_time * RATIO_OF_SECONDS_TO_MICROSECONDS);

  printf("Flow %2d finishes its transmission at time %.2f.\n", flow_to_transmit->id, get_elapsed_time());
}

double get_elapsed_time() {
  struct timeval current;
  if (gettimeofday(&current, NULL) == -1) 
    printf("Error getting time");

  double elapsedTime = (current.tv_sec - start.tv_sec) + ((current.tv_usec - start.tv_usec) / 1000000.0);
  return elapsedTime * 10;
}

void request_pipe(flow* flow) {

  if (pthread_mutex_lock(&execute_mutex) != 0)
    printf("Error with locking mutex");

  if (queue_root->on_pipe == 0 && is_empty(queue_root)) {
    add_to_flow_queue(flow);

    if (pthread_mutex_unlock(&execute_mutex) != 0)
      printf("Error with unlocking mutex");
    return;
  }

  add_to_flow_queue(flow);

  printf("Flow %2d waits for the finish of flow %2d. \n", flow->id, queue_root->id);

  while (queue_root->id != flow->id) {
    if (pthread_cond_wait(&execute_condvar, &execute_mutex) != 0)
      printf("Error waiting for condition variable");
  }

  queue_root->on_pipe = 1;

  if (pthread_mutex_unlock(&execute_mutex) != 0)
    printf("Error with unlocking mutex");
}

void release_pipe(flow* flow) {
  if (pthread_mutex_lock(&execute_mutex) != 0)
    printf("Error with locking mutex");

  dequeue();
  if (pthread_cond_broadcast(&execute_condvar) != 0)
    printf("Error broadcasting condition variable");

  if (pthread_mutex_unlock(&execute_mutex) != 0)
    printf("Error with unlocking mutex");
}

// Remove the flow from the top and let the next queue run
void dequeue() {
  queue_root = queue_root->next;
  queue_root->on_pipe = 1;
}

void add_to_flow_list(flow *new_flow) {
  flow *flow_to_add = initialize_node(new_flow);
  if (is_empty(flows_root)) {
    add_to_empty_list(flow_to_add);
  } else {
    add_to_non_empty_list(flow_to_add);
  }
}

void add_to_empty_list(flow *flow_to_add) {
  copy_flows(flow_to_add, flows_root);
  flows_root->next = initialize_node(NULL);
}

void add_to_non_empty_list(flow *flow_to_add) {
  flow* current_flow = get_end_of_list();
  copy_flows(flow_to_add, current_flow);
  current_flow->next = initialize_node(NULL);
}

flow* get_end_of_list() {
  flow *current_flow = flows_root;
  while (current_flow->priority != 0) {
    current_flow = current_flow->next;
  }
  return current_flow;
}

flow* initialize_node(flow *flow_to_copy) {
  flow* initialized_flow = (flow*)malloc(sizeof(flow));

  if (initialized_flow == NULL) {
    perror("Error: Out of memory");
  }

  if (flow_to_copy != NULL) {
    copy_flows(flow_to_copy, initialized_flow);
  } else {
    initialized_flow->priority = 0;
  }

  return initialized_flow;
}

void add_to_flow_queue(flow *flow_to_add) {
  flow *new_node = initialize_node(flow_to_add);

  if (is_empty(queue_root)) {
    add_to_empty_queue(new_node);
  } else {
    add_to_non_empty_queue(new_node);
  }
}

void add_to_empty_queue(flow *flow_to_add) {
  copy_flows(flow_to_add, queue_root);
  queue_root->on_pipe = 1;

  queue_root->next = initialize_node(NULL);
}

void add_to_non_empty_queue(flow *flow_to_add) {
  flow *previous_flow = queue_root;
  flow *current_flow = previous_flow->next;

  while (is_smaller_priority(flow_to_add, current_flow)) {
    previous_flow = current_flow;
    current_flow = current_flow->next;
  }
  if (position_of_flow(current_flow, previous_flow) == BEGINNING_OF_QUEUE) {

    add_to_front_of_queue(flow_to_add, current_flow);

  } else if (position_of_flow(current_flow, previous_flow) == MIDDLE_OF_QUEUE) {

    add_to_mid_of_queue(flow_to_add, current_flow, previous_flow);

  } else {
    add_to_end_of_queue(flow_to_add, current_flow);
  }

}

int position_of_flow(flow *current_flow, flow *previous_flow) {
  if (previous_flow == current_flow) {
    return 5;
  } else if (current_flow->priority != 0) {
    return 6;
  }
  return -1;
}

// Prioritize flows by their priority first, then arrival time, then transmission time, then the line number they appear on
bool is_smaller_priority(flow* flow_to_add, flow* current_flow) {
  if (current_flow->priority == 0) {
    return false;
  }
  if (current_flow->priority < flow_to_add->priority) {
    return true;
  } else if (current_flow->priority == flow_to_add->priority) {
    if (current_flow->arrival_time < flow_to_add->arrival_time) {
      return true;
    } else if (current_flow->arrival_time == flow_to_add->arrival_time) {
      if (current_flow->transmission_time < flow_to_add->transmission_time) {
        return true;
      } else if (current_flow->transmission_time == flow_to_add->transmission_time) {
        if (current_flow->line_number < flow_to_add->line_number) {
          return true;
        }
      }
    }
  }
  return false;
}

void add_to_front_of_queue(flow* flow_to_add, flow *current_flow) {
  queue_root = flow_to_add;
  queue_root->next = current_flow;
}

void add_to_mid_of_queue(flow* flow_to_add, flow *current_flow, flow *previous_flow) {
  previous_flow->next = flow_to_add;
  previous_flow->next->next = current_flow;
}

void add_to_end_of_queue(flow* flow_to_add, flow *current_flow) {
  copy_flows(flow_to_add, current_flow);

  current_flow->next = initialize_node(NULL);
}

void copy_flows(flow* source, flow* destination) {
  destination->id = source->id;
  destination->priority = source->priority;
  destination->arrival_time = source->arrival_time;
  destination->transmission_time = source->transmission_time;
  destination->line_number = source->line_number;
  destination->on_pipe = source->on_pipe;
}

bool is_empty(flow *root) {
  if (root == NULL || root->priority == 0)
    return true;
  else
    return false;
}

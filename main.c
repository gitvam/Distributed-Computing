#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
//#include "pthread_barrier.c"
#define RESET "\033[0m"
#define RED "\033[31m"
#define GREEN "\033[32m"

int N, M;  // number of threads

struct SinglyLinkedList {
    struct LLNode *head;
    struct LLNode *tail;
};

struct LLNode {
    int postID;
    pthread_mutex_t lock;
    bool marked;
    struct LLNode *next;
};


struct queue {
    struct queueNode *Head;
    struct queueNode *Tail;
    pthread_mutex_t headLock;
    pthread_mutex_t tailLock;
};


struct queueNode {
    int postID;
    struct queueNode *next;
};


struct Node
{
    struct Node *lc;
    struct Node *rc;
    int postID;
    bool IsLeftThreaded;
    bool IsRightThreaded;
    pthread_mutex_t lock;
};

struct Node *root=NULL;



pthread_barrier_t barrier_start_3rd_phase;
pthread_barrier_t barrier_start_3rd_phase_checks;

// Insert a Node in Binary Threaded Tree
struct Node *insert(struct Node *root, int ikey)
{
    // Searching for a Node with given value
    struct Node *ptr = root;
    struct Node *par = NULL; // Parent of key to be inserted


    while (ptr != NULL)
    {

        par = ptr; // Update parent pointer

        // Moving on left subtree.

        if (ikey < ptr->postID)
        {
            if (ptr -> IsLeftThreaded == false) {
                ptr = ptr->lc;
            }
            else{
                break;
            }
        }

            // Moving on right subtree.
        else
        {
            if (ptr->IsRightThreaded == false) {
                ptr = ptr->rc;

            }
            else{
                break;
            }
        }

    }

    // Create a new node
    struct Node *tmp = malloc(sizeof(struct Node));
    tmp -> postID = ikey;
    tmp -> IsLeftThreaded = true;
    tmp -> IsRightThreaded = true;
    pthread_mutex_init(&tmp->lock, NULL);

    pthread_mutex_lock(&par->lock);

    if (par->postID == -1)
    {
        pthread_mutex_lock(&tmp->lock);
        root = tmp;
        tmp -> lc = NULL;
        tmp -> rc = NULL;
        pthread_mutex_unlock(&par->lock);
        pthread_mutex_unlock(&tmp->lock);
    }
    else if (ikey < (par -> postID))
    {
        pthread_mutex_unlock(&par->lock);
        pthread_mutex_lock(&tmp->lock);
        tmp -> lc = par -> lc;
        tmp -> rc = par;
        par -> IsLeftThreaded = false;
        par -> lc = tmp;
        pthread_mutex_lock(&par->lock);
        pthread_mutex_unlock(&tmp->lock);
    }
    else
    {
        pthread_mutex_unlock(&par->lock);
        pthread_mutex_lock(&tmp->lock);
        tmp -> lc = par;
        tmp -> rc = par -> rc;
        par -> IsRightThreaded = false;
        par -> rc = tmp;
        pthread_mutex_lock(&par->lock);
        pthread_mutex_unlock(&tmp->lock);
    }

    pthread_mutex_unlock(&par->lock);
    return root;
}

// Returns inorder successor using rthread
struct Node *inorderSuccessor(struct Node *ptr)
{

    if (ptr -> IsRightThreaded == true){

        return ptr->rc;
    }

    ptr = ptr -> rc;

    while (ptr -> IsLeftThreaded == false) {
        ptr = ptr->lc;
    }
    pthread_mutex_unlock(&ptr->lock);
    return ptr;
}

// Printing the threaded tree
int count = 0;
void inorder(struct Node *root, int expexted)
{

    if (root == NULL)
        printf("Tree is empty");

    // Reach leftmost node
    struct Node *ptr = root;
    while (ptr -> IsLeftThreaded == false)
        ptr = ptr -> lc;


    while (ptr != NULL)
    {

        count++;
        ptr = inorderSuccessor(ptr);
    }

    printf("Third phase checks{\n");
    if (expexted == count) {
        printf(GREEN
        "\tTrees' total size finished (expected: %d, found: %d)\n" RESET,
                expexted, count);
        count=0;

    } else {
        printf(RED
        "\tTrees' total size finished (expected: %d, found: %d)\n" RESET,
                expexted, count);
        count=0;
        exit(-1);
    }
}


struct Node* inSucc(struct Node* ptr)
{
    pthread_mutex_unlock(&ptr->lock);
    if (ptr->IsRightThreaded == true) {
        pthread_mutex_lock(&ptr->lock);
        return ptr->rc;
    }

    ptr = ptr->rc;
    pthread_mutex_unlock(&ptr->lock);
    while (ptr->IsLeftThreaded == false)
        ptr = ptr->lc;

    return ptr;
}

struct Node* inPred(struct Node* ptr)
{
    pthread_mutex_unlock(&ptr->lock);
    if (ptr->IsLeftThreaded == true) {
        pthread_mutex_lock(&ptr->lock);
        return ptr->lc;
    }

    ptr = ptr->lc;
    pthread_mutex_unlock(&ptr->lock);
    while (ptr->IsRightThreaded == false)
        ptr = ptr->rc;

    return ptr;
}

struct Node* caseA(struct Node* root, struct Node* par,
                   struct Node* ptr)
{
    if (par == NULL)
        root = NULL;

    else if (ptr == par->lc) {
        par->IsLeftThreaded = true;
        par->lc = ptr->lc;
    }
    else {
        par->IsRightThreaded = true;
        par->rc = ptr->rc;
    }


    return root;
}


struct Node* caseB(struct Node* root, struct Node* par,
                   struct Node* ptr)
{
    struct Node* child;


    if (ptr->IsLeftThreaded == false) {
        child = ptr->lc;
        pthread_mutex_unlock(&ptr->lock);
    }

    else {
        child = ptr->rc;
        pthread_mutex_unlock(&ptr->lock);
    }

    pthread_mutex_lock(&root->lock);
    pthread_mutex_lock(&child->lock);
    if (par == NULL) {
        root = child;
        if(root)pthread_mutex_unlock(&root->lock);
        if(child)pthread_mutex_unlock(&child->lock);
    }

    else if (ptr == par->lc) {
        pthread_mutex_lock(&par->lock);
        par->lc = child;
        if(par)pthread_mutex_unlock(&par->lock);
    }
    else {
        pthread_mutex_lock(&par->lock);
        par->rc = child;
        if(par)pthread_mutex_unlock(&par->lock);
    }


    struct Node* s = inSucc(ptr);
    struct Node* p = inPred(ptr);

    if (ptr->IsLeftThreaded == false) {
        p->rc = s;

    }


    else {
        if (ptr->IsRightThreaded == false) {
            s->lc = p;
        }
    }
    pthread_mutex_unlock(&ptr->lock);

    return root;
}



struct Node* caseC(struct Node* root, struct Node* par,
                   struct Node* ptr)
{

    struct Node* parsucc = ptr;
    struct Node* succ = ptr->rc;


    while (succ->IsLeftThreaded==false) {
        if(parsucc)pthread_mutex_lock(&parsucc->lock);
        parsucc = succ;
        succ = succ->lc;
        if(parsucc)pthread_mutex_unlock(&parsucc->lock);
    }

    ptr->postID = succ->postID;

    if (succ->IsLeftThreaded == true && succ->IsRightThreaded == true)
        root = caseA(root, parsucc, succ);
    else
        root = caseB(root, parsucc, succ);

    return root;
}


bool delThreadedBST(struct Node* root, int dkey)
{

    struct Node *par = NULL, *ptr = root;


    int found = 0;


    while (ptr != NULL) {

        if (dkey == ptr->postID) {
            found = 1;
            break;
        }
        par = ptr;
        if(par)pthread_mutex_unlock(&par->lock);
        if (dkey < ptr->postID) {
            if(par)pthread_mutex_lock(&par->lock);
            if (ptr->IsLeftThreaded == false)
                ptr = ptr->lc;
            else
                break;
        }
        else {
            if(par)pthread_mutex_lock(&par->lock);
            if (ptr->IsRightThreaded == false)
                ptr = ptr->rc;
            else
                break;
        }

    }
    if(par)pthread_mutex_unlock(&par->lock);


    if (found == 0) {return 0;}

        // Two Children
    else if (ptr->IsLeftThreaded == false && ptr->IsRightThreaded == false) {
        pthread_mutex_lock(&ptr->lock);
        if(par)pthread_mutex_lock(&par->lock);
        root = caseC(root, par, ptr);
        pthread_mutex_unlock(&par->lock);
        if(par)pthread_mutex_unlock(&par->lock);
    }

        // Only lc Child
    else if (ptr->IsLeftThreaded == false) {
        if(par)pthread_mutex_lock(&par->lock);
        pthread_mutex_lock(&ptr->lock);
        root = caseB(root, par, ptr);
        pthread_mutex_unlock(&ptr->lock);
        if(par)pthread_mutex_unlock(&par->lock);
    }

        // Only rc Child
    else if (ptr->IsRightThreaded == false) {
        pthread_mutex_lock(&ptr->lock);
        if(par)pthread_mutex_lock(&par->lock);
        root = caseB(root, par, ptr);
        pthread_mutex_unlock(&ptr->lock);
        if(par)pthread_mutex_unlock(&par->lock);

    }

        // No child
    else {
        if(par)pthread_mutex_lock(&par->lock);
        pthread_mutex_lock(&ptr->lock);
        root = caseA(root, par, ptr);
        pthread_mutex_unlock(&ptr->lock);
        if(par)pthread_mutex_unlock(&par->lock);
    }

    //pthread_mutex_unlock(&par->lock);;
    if(par)pthread_mutex_unlock(&par->lock);
    pthread_mutex_unlock(&ptr->lock);

    return 1;
}


struct queueNode *new_queue_node(int key){
    struct queueNode *n = malloc(sizeof(struct queueNode));
    n->postID = key;
    n->next = NULL;
    return n;
}


struct queue *initialize_queue(){
    struct queue *temp = malloc(sizeof(struct queue));
    temp->Head = new_queue_node(-1);
    temp->Tail = temp->Head;
    pthread_mutex_init(&temp->headLock, NULL);
    pthread_mutex_init(&temp->tailLock, NULL);
    return temp;
}




void enqueue(struct queue *queue, int key) {
    struct queueNode *n = malloc(sizeof(struct queueNode)); //=new_queue_node(key);
    n->postID = key;
    n-> next = NULL;
    pthread_mutex_lock(&queue->tailLock);
    queue->Tail->next = n;
    queue->Tail = n;
    pthread_mutex_unlock(&queue->tailLock);
}

int dequeue(struct queue *queue) {
    int result;
    pthread_mutex_lock(&queue->headLock);
    if (queue->Head->next == NULL) //empty queue
        result = -1;
    else {
        result = queue->Head->next->postID;
        struct queueNode *tmp = queue->Head;
        queue->Head = queue->Head->next;
        free(tmp);
    }
    pthread_mutex_unlock(&queue->headLock);
    return result;
}



void queue_total_size_check(struct queueNode* queues, int expected, int i) {

    int count = -1;
    while(queues!=NULL){
        count++;
        queues = queues->next;
    }

    if (count == expected) {
        printf(GREEN
               "\tCategories[%d] queue's total size counted (expected: %d, found: %d)\n" RESET,
               i,expected, count);
    } else {
        printf(RED
               "\tCategories[%d] queue's total size counted (expected: %d, found: %d)\n" RESET,
               i,expected, count);
        exit(-1);
    }
}


int queue_sum_nodes(struct queueNode* node) {
    if (node == NULL) return 0;
    return queue_sum_nodes(node->next) + node->postID;
}

void queue_key_sum_check(struct queue** queues, int expected) {
    int Y = 0;
    int i;
    for (i = 0; i < N/4; i++) {
        Y += queue_sum_nodes(queues[i]->Head->next);
    }
    if (Y == expected) {
        printf(
                GREEN
                "\tQueues' total keysum counted (expected: %d, found: %d)\n" RESET,
                expected, Y);
    } else {
        printf(
                RED
                "\tQueues' total keysum counted (expected: %d, found: %d)\n" RESET,
                expected, Y);
        exit(-1);
    }
}

bool validate(struct LLNode *pred, struct LLNode *curr) {
    if (pred->marked == false && curr->marked == false && pred->next == curr)
        return true;
    else return false;
}


struct SinglyLinkedList *init_list(void)
{
    struct SinglyLinkedList *list;

    if ((list = (struct SinglyLinkedList *) calloc(1, sizeof(struct SinglyLinkedList))) == NULL) {

        return NULL;
    }

    if ((list->head = (struct LLNode *) calloc(1, sizeof(struct LLNode))) == NULL) {

        goto end;
    }
    list->head->lock = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;

    if ((list->tail = (struct LLNode *) calloc(1, sizeof(struct LLNode))) == NULL) {

        goto end;
    }
    list->tail->lock = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;

    list->head->next = list->tail;
    list->tail->next = NULL;

    list->head->postID = -9999999;
    list->tail->postID = 9999999;

    return list;

    end:
    free (list->head);
    free (list);
    return NULL;
}


static struct LLNode *create_node(int key)
{
    struct LLNode *node;

    if ((node = calloc(1, sizeof(struct LLNode))) == NULL) {
        return NULL;
    }

    node->postID = key;
    node->marked = false;
    node->lock = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;

    return node;
}


bool LL_insert(struct SinglyLinkedList * l, int key)
{
    struct LLNode *pred, *curr;
    struct LLNode *newNode;
    bool result = true;
    bool return_flag = false;

    if ((newNode = create_node(key)) == NULL)
        return false;

    while (1) {
        pred = l->head;
        curr = pred->next;

        while (curr->postID < key && curr != l->tail) {
            pred = curr;
            curr = pred->next;
        }

        pthread_mutex_lock(&pred->lock);
        pthread_mutex_lock(&curr->lock);

        if (validate(pred, curr)) {
            if (key == curr->postID) {
                result = false;
                return_flag=1;
            } else {
                newNode = malloc(sizeof(struct LLNode));
                newNode->next = curr;
                pred->next = newNode;
                newNode->postID = key;
                result = true;
                return_flag=true;
            }
        }
        pthread_mutex_unlock(&pred->lock);
        pthread_mutex_unlock(&curr->lock);
        if (return_flag) return result;
    }

}


bool LL_delete(struct LLNode* head, int key)
{
    // code for process p
    struct LLNode *pred, *curr;
    bool result; bool return_flag = 0;
    while (1) {
        pred = head; curr = pred->next;
        while (curr->postID < key) {
            pred = curr;
            curr = curr->next;
        }

        pthread_mutex_lock(&pred->lock);
        pthread_mutex_lock(&curr->lock);
        if (validate(pred, curr)) {
            if (key == curr->postID) {
                curr->marked = true;
                pred->next = curr->next;
                result = true;
            }
            else result = false;
            return_flag = 1;
        }
        pthread_mutex_unlock(&pred->lock);
        pthread_mutex_unlock(&curr->lock);
        if (return_flag == 1) return result;
    }
}




struct SinglyLinkedList* list;


void list_total_keysum_check(int expected){
    struct LLNode *curr = list->head;
    int Y=0;
    while (curr!=list->tail) {
        if(curr!=list->head)Y += curr->postID;
        curr = curr->next;
    }

    if (Y == expected) {
        printf(GREEN
               "\tList's total keysum counted (expected: %d, found: %d)\n" RESET,
               expected, Y);
    } else {
        printf(RED
               "\tList's total keysum counted (expected: %d, found: %d)\n" RESET,
               expected, Y);
        exit(-1);
    }
}


void list_total_size_check(int expected) {
    struct LLNode *curr = list->head;
    int Y=0;
    while (curr!=list->tail) {
        if(curr!=list->head)Y++;
        curr = curr->next;
    }

    if (Y == expected) {
        printf(GREEN
               "\tList's total size counted (expected: %d, found: %d)\n" RESET,
               expected, Y);
    } else {
        printf(RED
               "\tList's total size counted (expected: %d, found: %d)\n" RESET,
               expected, Y);
        exit(-1);
    }
}



pthread_barrier_t barrier_start_1st_phase_checks;
pthread_barrier_t barrier_start_2nd_phase;
pthread_barrier_t barrier_start_2nd_phase_end;
pthread_barrier_t barrier_start_3rd_phase;
pthread_barrier_t barrier_start_3rd_phase_checks;
pthread_barrier_t barrier_start_4th_phase;
pthread_barrier_t barrier_start_4th_phase_checks;

struct queue **Categories;

void *run(void *arg){
    int index = -1;
    int i;
    int thread_no = (int)arg;
    for (i = 0; i < N; i++) {
        index = thread_no + i*2*N;
        LL_insert(list, index);
    }
    pthread_barrier_wait(&barrier_start_1st_phase_checks);
    if (thread_no == 0) {
        printf("First phase checks {\n");
        list_total_size_check(2*(N * N));
        list_total_keysum_check(2*(N * N * N * N)- (N * N));
        printf("}\n");
    }
}

void *run2(void *arg){
    int thread_no = (int)arg;
    pthread_barrier_wait(&barrier_start_2nd_phase);
    for(int j=0; j<N; j++) {
        enqueue(Categories[((thread_no) % N/4)], thread_no + (j * M));
        enqueue(Categories[((thread_no) % N/4)], thread_no + (j * M + N));
        LL_delete(list->head,thread_no + (j * M));
        LL_delete(list->head,thread_no + (j * M + N));
    }

    pthread_barrier_wait(&barrier_start_2nd_phase_end);
    if (thread_no == 0) {
        printf("Second phase checks{\n");
        for(int k=0; k<N/4; k++)
            queue_total_size_check(Categories[k]->Head, 8 * N,k);
        queue_key_sum_check(Categories, 2*(N * N * N * N) - (N * N));
        list_total_size_check(0);
        printf("}\n");
    }
}


void *run3(void *arg){
    pthread_barrier_wait(&barrier_start_3rd_phase);
    int thread_no = (int)arg;
    for(int i=0; i<N; i++){
        root = insert(root, Categories[((thread_no) % N/4)]->Tail->postID);
        dequeue(Categories[((thread_no) % N/4)]);
    }

    pthread_barrier_wait(&barrier_start_3rd_phase_checks);
    if(thread_no==N) {
        inorder(root,N*N);
        for(int k=0; k<N/4; k++)
            queue_total_size_check(Categories[k]->Head, 4 * N,k);
        printf("}\n");
    }
}



int ok=0;
void *run4(void *arg){
    pthread_barrier_wait(&barrier_start_4th_phase);
    int thread_no = (int)arg;
    for(int i=0; i<N; i++){
        /*if delete == true, it deleted ok times */
        if(delThreadedBST(root,Categories[((thread_no) % N/4)]->Tail->postID))ok++;
        enqueue(Categories[((thread_no) % N/4)], Categories[((thread_no) % N/4)]->Tail->postID);
    }

    pthread_barrier_wait(&barrier_start_4th_phase_checks);
    if(thread_no == 0) {

        printf("Fourth phase checks{\n");
        if (ok == N * N) {
            printf(GREEN
                   "\tTrees' total size finished (expected: %d, found: %d)\n" RESET,
                   0, (N*N - ok));
        } else {
            printf(RED
                   "\tTrees' total size finished (expected: %d, found: %d)\n" RESET,
                   0, (N*N - ok));
            printf("}\n");
           exit(-1);
        }
        for(int k=0; k<N/4; k++)
            queue_total_size_check(Categories[k]->Head, 8 * N,k);

       // queue_key_sum_check(Categories, 2*(N * N * N * N) - (N * N));
        printf("}\n");
    }

}

int main(int argc, char **argv)
{
    if (argc != 2) {
      fprintf(stderr, "Insufficient number of arguments given,exiting\n");
      exit(-1);
    }
    N = atoi(argv[1]);
    M = N * 2;
    int i=0;

    pthread_barrier_init(&barrier_start_1st_phase_checks, NULL, M);
    pthread_barrier_init(&barrier_start_2nd_phase, NULL, M/2);
    pthread_barrier_init(&barrier_start_2nd_phase_end, NULL, M/2);
    pthread_barrier_init(&barrier_start_3rd_phase, NULL, N);
    pthread_barrier_init(&barrier_start_3rd_phase_checks, NULL, N);
    pthread_barrier_init(&barrier_start_4th_phase, NULL, N);
    pthread_barrier_init(&barrier_start_4th_phase_checks, NULL, N);
    pthread_t threads[M];

    list = init_list();
    Categories = malloc((N/4) * sizeof(struct queue *));
    for (i = 0; i < N/4; i++) {
        Categories[i] = initialize_queue();
    }
    // -- A phase
    for (i = 0; i < M; i++) {
        pthread_create(&threads[i], NULL, run, (void *)i);
    }

    for (i = 0; i < M; i++) {
        pthread_join(threads[i], NULL);
    }
    // -- B phase
    for (i = 0; i < N; i++) {
        pthread_create(&threads[i], NULL, run2, (void *)i);
    }

    for (i = 0; i < N; i++) {
        pthread_join(threads[i], NULL);
    }

    // -- C phase
    root = malloc(sizeof(struct Node));
    root->lc=NULL;
    root->rc=NULL;
    root->postID=-1;
    pthread_mutex_init(&root->lock, NULL);


    for (int i = N; i < M; i++) {
        pthread_create(&threads[i], NULL, run3, (void *)i);
    }

    for (int i = N; i < M; i++) {
        pthread_join(threads[i], NULL);
    }

    // D phase
    for (int i = 0; i < N; i++) {
        pthread_create(&threads[i], NULL, run4, (void *)i);
    }

    for (int i = 0; i < N; i++) {
        pthread_join(threads[i], NULL);
    }
    return 0;
}

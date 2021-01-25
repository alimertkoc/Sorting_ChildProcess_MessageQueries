
/*
    Muhammet Onur Bayraktar 170419821
    Bahadır Şahin           170419822
    Ali Mert Koç            170418018
*/

/*
    fork() ve POSIX Message Queues
    kullanarak N adet dosya icerisindeki
    k. en buyuk sayiyi bulan ve buldugu
    degerleri bir cikti dosyasina yazan
    programdir.     
*/


#include <stdio.h>
#include <fcntl.h>
#include <mqueue.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <sys/wait.h>

// Mesaj kuyruklarinin ozelliklerini
// tutan struct yapisidir
struct mq_attr attributes = {
    .mq_msgsize = 15,
    .mq_maxmsg = 10,
    .mq_curmsgs = 0,
    .mq_flags = 0
};

/*
    k. en buyuk elemani bulan fonksiyondur.
    Bubble sort algoritmasi k + 1 kez calistirilinca
    k. en buyuk eleman dizinin sondan k. elemani olur.
*/
int findTopK(int *arr, int k, int n)
{
    int tmp = 0;

    for (int i = 0; i < k + 1; i++) // dongu k + 1 kez calistirilacak
    {
        for (int j = 0; j < n - i - 1; j++)
        {
            // su anki eleman sonrakinden buyukse yer degistirir
            if (arr[j] > arr[j + 1])
            {
                tmp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = tmp;
            }
        }
    }

    return arr[n - k - 1]; // Sondan k. elemani dondurur
}

/*
    Her bir child processten gelen int degerlerinin
    dosyaya yazilmadan once buyukten kucuge siralanmasi
    icin yazildi.
*/
void sort(int *arr, int n)
{
    int tmp;

    for (int i = 0; i < n - 1; i++)
    {
        for (int j = 0; j < n - i - 1; j++)
        {
            if (arr[j] < arr[j + 1])
            {
                tmp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = tmp;
            }
        }
    }
}

/*
    Girdi dosyalarindan sayi degerlerini alan
    ve aldigi degerlerle birlikte, bu degerleri
    tutan dizinin boyutunu donduren fonksiyondur.
*/

int** getNumbersFromFile(char* filename) {
    int fd = open(filename, O_RDONLY);
    int** return_val = (int**)malloc(sizeof(int*) * 2);
    static int inputs[INT_MAX];
    int input_index = 0, tmp_num_index = 0;
    char tmp_c, tmp_num[11];

    while(read(fd, &tmp_c, 1) != 0) {
        if(tmp_c == ' ') {
            tmp_num[tmp_num_index] = '\0';
            tmp_num_index = 0;
            sscanf(tmp_num, "%d", &inputs[input_index]);
            input_index++;
        }

        else {
            tmp_num[tmp_num_index] = tmp_c;
            tmp_num_index++;
        }
    }

    close(fd);

    // Ayni anda iki degisken dondurebilmek icin
    // bu degiskenlerin adreslerini tutan bir dizi
    // olusturuldu ve donduruldu.
    return_val[0] = &input_index;
    return_val[1] = inputs;
    return return_val;
}


/*
    Verilen integer tipindeki degiskeni siraya ekler
*/
void addToQueue(int topK) {
    // Sira yazma modunda acildi.
    mqd_t mq = mq_open("/testqueue", O_WRONLY);
    
    // Arguman olarak verilen sayi stringe cevrildi
    char text[20];
    sprintf(text, "%d", topK);

    if(mq == -1) { // Sira acilamazsa
        perror("Failed to open queue");
        exit(1);
    }

    // mq_send fonksiyonu bir problem cikarsa -1 dondurur
    if(mq_send(mq, text, strlen(text) + 1, 1) == -1) {
        perror("Failed to add to queue");
        exit(1);
    }

    // Kuyruk kapatildi
    mq_close(mq);
    
}

void findTopKMQueue(int k, int N, char filenames[][20]) {
    int* inputs;
    int topK;
    int** output;
    
    for(int i = 0; i < N; i++) {
        if(fork() == 0) {
            output = getNumbersFromFile(filenames[i]);
            topK = findTopK(output[1], k, *output[0]);
            addToQueue(topK);
            exit(0);
        }
    }

    for(int i = 0; i < N; i++)
        wait(NULL);
    
}

void printArray(int *arr, int n)
{
    for (int i = 0; i < n - 1; i++)
    {
        printf("%d, ", arr[i]);
    }

    printf("%d\n", arr[n - 1]);
}

void main(int argc, char *argv[]) {

    /*
        Program execute edilirken verilen
        argumanlar degiskenlere atanir.
    */
    int k, N;

    sscanf(argv[1], "%d", &k);
    sscanf(argv[2], "%d", &N);

    int outputs[N];

    char filenames[N][20], outfile[20];

    for (int i = 3; i < argc - 1; i++)
    {
        strcpy(filenames[i - 3], argv[i]);
    }

    strcpy(outfile, argv[argc - 1]);

    // Daha once bu isimle acilan bir sira varsa siler.
    mq_unlink("/testqueue");

    // Sirayi yazma ve okuma izni ile olusturur.
    mqd_t mq = mq_open("/testqueue", O_CREAT | O_RDWR, 0644, &attributes);
    
    if(mq == -1) {
        perror("Failed to open queue");
        exit(1);
    }
    
    findTopKMQueue(k, N, filenames);
    
    // Child processler bittikten sonra
    // Sira okuma modunda tekrar acilir.
    mq = mq_open("/testqueue", O_RDONLY);
    
    // Siradan okunacak degerleri tutan dizi
    char recvd_text[30];

    if(mq == -1) {
        perror("Failed to open queue");
        exit(1);
    }
    
    for(int i = 0; i < N; i++) {
        // Sirayi okuma islemi N kez tekrarlanir
        if(mq_receive(mq, recvd_text, 20, NULL) == -1) {
            perror("Failed to receive from queue");
            exit(1);
        }

        //i Sradan alinan degerler diziye atanir.
        sscanf(recvd_text, "%d", &outputs[i]);
    }

    

    sort(outputs, N); // buyukten kucuge siralama
    printf("****POSIX Message Queues****\n"); // make komutunun testi icin
    // printArray(outputs, N);

    /* Ciktilari dosyaya yazma islemi */
    int out_fd = open(outfile, O_CREAT | O_WRONLY, S_IRUSR);
    char result[11];

    for (int k = 0; k < N; k++)
    {
        sprintf(result, "%d", outputs[k]);
        strcat(result, "\n");
        write(out_fd, result, strlen(result));
    }

    close(out_fd);

    mq_close(mq);
    mq_unlink("/testqueue");
}
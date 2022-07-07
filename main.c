#include <stdio.h>
#include "mpi.h"
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <ctype.h>

int main(int argc, char **argv){

    int rank, size;
    


    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Status status;
    int tag=0;
    char Message[255];
    if(rank==0)
    {
        int nrCores=6; 
        int sCores=5;
        int coreArray[]={0,0,0,0,0,0};

        int check;
  
        check = mkdir("temp",0777);
  
       
        if (!check)
            printf("Directory created\n");
        else 
        {
            printf("Unable to create directory\n");
            exit(1);
        }
        char *dir="test-files";
        

        DIR *dp;
        struct dirent *entry;
        struct stat statbuf;
        if((dp = opendir(dir)) == NULL) {
            fprintf(stderr,"cannot open directory: %s\n", dir);
        }
        else
        {
            chdir(dir);
            while((entry = readdir(dp)) != NULL) {
                lstat(entry->d_name,&statbuf);
                    if(strcmp(".",entry->d_name) == 0 ||
                        strcmp("..",entry->d_name) == 0)
                        continue;
                    while(sCores<=0)
                    {
                        MPI_Recv(Message,255,MPI_CHAR,MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
                        sCores++;
                        coreArray[status.MPI_SOURCE]=status.MPI_TAG;
                    }
                    for(int i=1;i<nrCores;++i)
                    {
                        if(coreArray[i]==0)
                        {
                            sprintf(Message,"test-files/%s",entry->d_name);
                            MPI_Send ( Message , 255 , MPI_CHAR, i , tag , MPI_COMM_WORLD );
                            sCores--;
                            coreArray[i]=1;
                            break;
                        }
                        
                    }
                
            }
            for(int i=1;i<nrCores;++i)
            {
                sprintf(Message," ");
                tag=2;
                MPI_Send ( Message , 255 , MPI_CHAR, i , tag , MPI_COMM_WORLD );
                   
            }
            chdir("..");
            closedir(dp);
            
        }
        for(int i=1;i<nrCores;++i)
        {
            int statre=0;
            MPI_Recv(&statre,1,MPI_INT,i,2,MPI_COMM_WORLD,&status);
            sCores++;
        }
        printf("%d\n", sCores);
        
        //REDUCE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        char *tempdir="temp";
        if((dp = opendir(tempdir)) == NULL) {
            fprintf(stderr,"cannot open directory: %s\n", dir);
        }
        else
        {
            chdir(tempdir);
            while((entry = readdir(dp)) != NULL) {
                lstat(entry->d_name,&statbuf);
                    if(strcmp(".",entry->d_name) == 0 ||
                        strcmp("..",entry->d_name) == 0)
                        continue;
                        printf("%s\n",entry->d_name);
            	//reduce code here
                   
            }
            chdir("..");
            closedir(dp);
            system("rm -rf temp");
        }
    }
    else
    {
        while(tag!=2)
        {

         MPI_Recv(Message,255,MPI_CHAR,MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,&status);
         if(status.MPI_TAG==2)
         {
             tag=status.MPI_TAG;
             break;
         }
         FILE *fp;
         FILE *fpc;
        
        
            char buff[255];
            
            fp = fopen(Message, "r");
            char* fileName;
            fileName=strtok (Message,"/");
            fileName=strtok(NULL, "/");
            while(fgets(buff,255,fp)!=NULL)
            {
                
                char pathcreate[255];
                char *token;
                
                token = strtok (buff," \t,.-\n()[]{}*\";?!1234567890/@#$%%^&_+=:<>|~`\r");
                while (token != NULL)
                {
                
                    char *lower=token;
                    while(*lower) {
                        *lower = tolower(*lower);
                        lower++;
                    }
                    sprintf(pathcreate,"temp/%s_%s",token,fileName);
                    fpc = fopen(pathcreate,"w");
                    fprintf(fpc,"%d\n",rank);
                    fclose(fpc);
                    token = strtok (NULL, " \t,.-\n()[]{}*\";?!1234567890/@#$%%^&_+=:<>|~`\r");
                }
            }

            fclose(fp);
            
            
            tag=0;
            sprintf(Message," ");
            MPI_Send ( Message , 255 , MPI_CHAR, 0 , tag , MPI_COMM_WORLD );

        
        }
        int stat=0;
        MPI_Send ( &stat , 1 , MPI_INT, 0 , tag , MPI_COMM_WORLD );
        // while(tag==2)
        // {
            
        // }


    }
    MPI_Finalize();
    

    return 0;
}

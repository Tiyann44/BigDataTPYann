package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob1 {
    public static void main(String[] args) throws Exception {
        // Vérification des arguments : on attend 2 arguments, le chemin d'entrée et le chemin de sortie
        if (args.length != 2) {
            System.err.println("Usage: ColFilJob1 <input path> <output path>");
            System.exit(-1);
        }

        // Configuration du job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ColFilJob1");

        // Définir la classe principale, le Mapper, le Reducer, l'InputFormat
        job.setJarByClass(ColFilJob1.class);
        job.setMapperClass(RelationshipMapper.class);   // Le Mapper
        job.setReducerClass(RelationshipReducer.class); // Le Reducer

        // Définir les types de sortie (key, value) pour le Mapper et le Reducer
        job.setOutputKeyClass(Text.class);     // La clé est de type Text (identifiant utilisateur)
        job.setOutputValueClass(Text.class);   // La valeur est de type Text (liste d'amis)

        // Utiliser notre InputFormat personnalisé
        FileInputFormat.addInputPath(job, new Path(args[0])); // Chemin d'entrée
        job.setInputFormatClass(RelationshipInputFormat.class); // Spécifier notre InputFormat personnalisé

        // Définir le chemin de sortie
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Chemin de sortie

        // Utilisation de deux reducers
        job.setNumReduceTasks(2);

        // Lancer le job et attendre qu'il se termine
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

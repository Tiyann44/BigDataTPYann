package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonRelationsMapper extends Mapper<Object, Text, UserPair, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Format d'entrée : utilisateur -> relation1,relation2,relation3
        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            return; // Ignorer les lignes mal formées
        }

        String user = line[0]; // Utilisateur source
        String[] relations = line[1].split(","); // Relations directes

        // Générer toutes les paires possibles entre les relations
        for (int i = 0; i < relations.length; i++) {
            for (int j = i + 1; j < relations.length; j++) {
                UserPair pair = new UserPair(relations[i], relations[j]);
                context.write(pair, new Text(user));
            }
        }

        // Émettre les relations directes pour les filtrer dans le reducer
        for (String relation : relations) {
            UserPair directPair = new UserPair(user, relation);
            context.write(directPair, new Text("DIRECT"));
        }
    }
}

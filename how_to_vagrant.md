Testé avec vagrant 2.0.3 et oracle virtual box 5.2.8

Si besoin, nous avons avec nous les installs `vagrant_2.0.3_x86_64.msi` et `VirtualBox-5.2.8-121009-Win.exe`



Copier les fichiers `devoxx2018_v1.2.box` et `Vagrantfile` depuis la clef usb vers le répertoire de travail de votre choix.



Dans votre repertoire de travail executez la commande suivante :

`vagrant box add devoxx2018_v1.2.box --name devoxx_box`

Vous devriez voir un message se terminant par 
```==> box: Successfully added box 'devoxx_box' (v0) for 'virtualbox'!```


Vagrant va monter la box dans virtual box et la booter.

Arrêtez et re-lancez la:
`vagrant halt`

`vagrant up`

Faites un `vagrant ssh` pour vous connecter à la VM.



Quelques commandes utiles :

`sudo ccm create devoxx2018 -v binary:3.0.16 -n 3`

`sudo ccm start`

`sudo ccm status`

`sudo ccm node1 cqlsh`

`sudo ccm node1 nodetool info`

`sudo ccm node1 nodetool status`


Vagrant propose un repertoire partagé avec votre OS sous le repertoire `/vagrant` dans votre box 

Exemple d'utilisation:
`/vagrant/cdm install movielens`

et

`java -jar /vagrant/target/devoxx2018-0.0.1-SNAPSHOT.jar`







 

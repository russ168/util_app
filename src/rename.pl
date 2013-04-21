#!/usr/bin/perl -i

@file_list = `find . -name "*.jpg"`;
$i = 1;
foreach $file (@file_list){
	chomp($file);
	print "$file\n";
	$file =~ /.*\.(.*)/ ;
  $newfile = "$i.$1";
  print "$newfile\n";
  `mv $file $newfile`;
  $i = $i+1;
   }


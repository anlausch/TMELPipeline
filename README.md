# TMELPipeline
This project is a Python implementation of Entity-based Topic Modeling, a new corpus exploration method. The tool identifies a series of descriptive labels for each document in an ontology and generates for each label a topic, which is easy to
interpret as it is directly linked to a knowledge base.

The interconnected web-based evaluation platform for gathering annotations and statistics related to the results of the pipeline is available for download on Github via this link: https://github.com/anlausch/TMEvaluationPlatform.

Installation instructions:
- install Python 2.7
- install Java 7 (that is required by the Stanford TMT!)
- install Scala
- install Python NLTK 3.2
- run the following commands in the Python interpreter:
	>> import nltk
	>> nltk.download('punkt')
- install MySQL
- install mysql-connector for Python
- change configuration in settings file to your needs
- run program

This project was part of the research that was done on Entity-based Topic Modeling by the Data and Web Science Research Group of the University of Mannheim. More information about our work can be found here:
http://dws.informatik.uni-mannheim.de/en/home/.

Please do not forget to cite our work when using it in your project:
Anne Lauscher, Federico Nanni, Pablo Ruiz Fabo and Simone Paolo Ponzetto Entities as Topic Labels: Combining Entity Linking and Labeled LDA to Improve Topic Interpretability and Evaluability. In: Italian Journal of Computational Linguistics 2(2), pp. 67-88, 2016.

![](http://www.uni-mannheim.de/1/english/config/uni_ma_logo_engl.gif)

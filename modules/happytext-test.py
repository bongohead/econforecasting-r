from happytransformer import HappyTextClassification

happy_tc = HappyTextClassification("DISTILBERT", "distilbert-base-uncased-finetuned-sst-2-english", num_labels=2)
result = happy_tc.classify_text("Caramel is a good dog")
print(result.label)

from wordcloud import WordCloud
from collections import Counter
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords
import string
import math
import pandas as pd
from googletrans import Translator
import time
import pickle
import os

# Initialize translator and download stopwords
translator = Translator()
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))
russian_stopwords = set(stopwords.words('russian'))

# Load translation cache if exists, else initialize an empty one
cache_file = "translation_cache.pkl"
if os.path.exists(cache_file):
    with open(cache_file, "rb") as f:
        translation_cache = pickle.load(f)
else:
    translation_cache = {}

def preprocess_text(text, n=1, lang='english'):
    tokens = text.lower().split()
    
    # Set stop words based on language
    if lang == 'ru':
        selected_stop_words = russian_stopwords
    else:
        selected_stop_words = stop_words
    
    tokens = [word.strip(string.punctuation) for word in tokens if word not in selected_stop_words and word.isalpha()]
    
    if n > 1:
        ngrams = zip(*[tokens[i:] for i in range(n)])
        tokens = [' '.join(ngram) for ngram in ngrams]
    
    return tokens

def translate_top_words(word_freq, top_k=30):
    top_words = [word for word, _ in word_freq.most_common(top_k)]
    translated_words = []

    for word in top_words:
        if word in translation_cache:
            translated_words.append(translation_cache[word])
        else:
            try:
                translation = translator.translate(word, src='ru', dest='en')
                translated_text = translation.text
                translation_cache[word] = translated_text  # Cache the translation
                translated_words.append(translated_text)
            except Exception as e:
                print(f"Translation error for word '{word}': {e}. Using original word.")
                translated_words.append(word)
                time.sleep(0.5)  # Delay to avoid rate limiting

    # Save updated cache to disk
    with open(cache_file, "wb") as f:
        pickle.dump(translation_cache, f)

    # Create a Counter with translated words
    translated_word_freq = Counter({translated_word: word_freq[word] for translated_word, word in zip(translated_words, top_words)})
    return translated_word_freq

def generate_word_clouds(data, start_date=None, end_date=None, division=3, interval_type=None, n=1, lang='english'):
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    start_date = pd.to_datetime(start_date).tz_localize(data['timestamp'].dt.tz) if start_date else data['timestamp'].min()
    end_date = pd.to_datetime(end_date).tz_localize(data['timestamp'].dt.tz) if end_date else data['timestamp'].max()
    
    data = data[(data['timestamp'] >= start_date) & (data['timestamp'] <= end_date)]

    if interval_type == 'year':
        unique_intervals = sorted(data['year'].unique())
        interval_col = 'year'
    elif interval_type == 'month':
        unique_intervals = sorted(data['month'].unique())
        interval_col = 'month'
    else:
        total_duration = (end_date - start_date) / division
        unique_intervals = [(start_date + i * total_duration, start_date + (i + 1) * total_duration) for i in range(division)]
        interval_col = None

    num_intervals = len(unique_intervals)
    cols = 3
    rows = math.ceil(num_intervals / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(cols * 5, rows * 4))
    axes = axes.flatten()

    for idx, interval in enumerate(unique_intervals):
        if interval_col == 'year':
            interval_data = data[data[interval_col] == interval]
            interval_label = f"Year: {interval}"
        elif interval_col == 'month':
            interval_data = data[data[interval_col] == interval]
            interval_label = f"Month: {interval}"
        else:
            interval_start, interval_end = interval
            interval_data = data[(data['timestamp'] >= interval_start) & (data['timestamp'] < interval_end)]
            interval_label = f"From {interval_start.date()} to {interval_end.date()}"

        text_data = ' '.join(interval_data['text'].dropna())

        if text_data.strip():
            words = preprocess_text(text_data, n=n, lang=lang)
            word_freq = Counter(words)

            if lang == 'ru':
                word_freq = translate_top_words(word_freq, top_k=30)

            wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_freq)
            axes[idx].imshow(wordcloud, interpolation='bilinear')
            axes[idx].axis('off')
            axes[idx].set_title(f'{n}-gram Word Cloud for {interval_label}')
        else:
            axes[idx].text(0.5, 0.5, 'No Data', ha='center', va='center', fontsize=16, color='gray')
            axes[idx].axis('off')
            axes[idx].set_title(f'{n}-gram Word Cloud for {interval_label}')

    for j in range(idx + 1, len(axes)):
        axes[j].axis('off')

    plt.tight_layout()
    plt.show()

def plot_top_ngrams(data, start_date=None, end_date=None, division=3, interval_type=None, n=1, top_k=10, lang='english'):
    """
    Plot a histogram of the most frequent words or n-grams in the textual data.
    """
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    start_date = pd.to_datetime(start_date).tz_localize(data['timestamp'].dt.tz) if start_date else data['timestamp'].min()
    end_date = pd.to_datetime(end_date).tz_localize(data['timestamp'].dt.tz) if end_date else data['timestamp'].max()
    
    data = data[(data['timestamp'] >= start_date) & (data['timestamp'] <= end_date)]

    if interval_type == 'year':
        unique_intervals = sorted(data['year'].unique())
        interval_col = 'year'
    elif interval_type == 'month':
        unique_intervals = sorted(data['month'].unique())
        interval_col = 'month'
    else:
        total_duration = (end_date - start_date) / division
        unique_intervals = [(start_date + i * total_duration, start_date + (i + 1) * total_duration) for i in range(division)]
        interval_col = None

    num_intervals = len(unique_intervals)
    cols = 3
    rows = math.ceil(num_intervals / cols)
    
    # Increased figure size for better visibility
    fig, axes = plt.subplots(rows, cols, figsize=(cols * 7, rows * 6))
    axes = axes.flatten()

    for idx, interval in enumerate(unique_intervals):
        if interval_col == 'year':
            interval_data = data[data[interval_col] == interval]
            interval_label = f"Year: {interval}"
        elif interval_col == 'month':
            interval_data = data[data[interval_col] == interval]
            interval_label = f"Month: {interval}"
        else:
            interval_start, interval_end = interval
            interval_data = data[(data['timestamp'] >= interval_start) & (data['timestamp'] < interval_end)]
            interval_label = f"From {interval_start.date()} to {interval_end.date()}"

        text_data = ' '.join(interval_data['text'].dropna())

        if text_data.strip():
            words = preprocess_text(text_data, n=n, lang=lang)
            word_freq = Counter(words)

            if lang == 'ru':
                word_freq = translate_top_words(word_freq, top_k=top_k)

            top_ngrams = word_freq.most_common(top_k)
            ngrams, counts = zip(*top_ngrams) if top_ngrams else ([], [])

            # Plot histogram with adjustments
            axes[idx].barh(ngrams, counts, color='skyblue')
            axes[idx].set_xlabel('Frequency')
            axes[idx].set_title(f'Top {top_k} {n}-grams for {interval_label}')
            axes[idx].invert_yaxis()

            # Set tight layout adjustments for better readability
            for label in axes[idx].get_yticklabels():
                label.set_fontsize(10)
            axes[idx].tick_params(axis='y', labelrotation=0)
        else:
            axes[idx].text(0.5, 0.5, 'No Data', ha='center', va='center', fontsize=16, color='gray')
            axes[idx].axis('off')
            axes[idx].set_title(f'{n}-gram Histogram for {interval_label}')

    for j in range(idx + 1, len(axes)):
        axes[j].axis('off')

    plt.tight_layout()
    plt.show()




# add main
if __name__ == '__main__':
    df_ru = pd.read_feather('DataFrames/Oblast_ru.feather')
    generate_word_clouds(df_ru, n=2 , division=3, start_date="2014-01-01", end_date="2024-01-01")  # Generates unigram word clouds
    plot_top_ngrams(df_ru, interval_type='year', n=3, top_k=10)
    


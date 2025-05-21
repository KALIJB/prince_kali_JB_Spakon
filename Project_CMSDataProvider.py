{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyPQ7nV2gfJZeB855i9OukZJ",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/KALIJB/prince_kali_JB_Spakon/blob/master/Project_CMSDataProvider.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 1,
      "metadata": {
        "id": "8ClmPNMddB-r"
      },
      "outputs": [],
      "source": [
        "import requests\n",
        "import json\n",
        "import gc\n",
        "import time\n",
        "import pandas as pd\n",
        "\n",
        "\n",
        "class CMSDATA:\n",
        "\n",
        "    def __init__(self):\n",
        "        self.url = \"https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false\"\n",
        "\n",
        "\n",
        "    def fetch_data(self):\n",
        "        all_data = []\n",
        "        retry_count = 0\n",
        "        max_retries = 3\n",
        "\n",
        "\n",
        "        while retry_count < max_retries:\n",
        "            try:\n",
        "                response = requests.get(self.url)\n",
        "                if response.status_code == 200:\n",
        "                    total_records = int(response.json()['count'])\n",
        "                    print(\"total records:\",total_records)\n",
        "                    # total_records = 100 #use for testing\n",
        "                    counter = 0\n",
        "                    while counter < total_records:\n",
        "                        limit = 2000\n",
        "                        offset_url = f\"https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset={counter}&count=true&results=true&schema=true&keys=true&format=json&rowIds=false\"\n",
        "                        offset_response = requests.get(offset_url)\n",
        "                        if offset_response.status_code == 200:\n",
        "                            print(f\"Made request {limit} results at offset {counter}\")\n",
        "                            offset_data = list(\n",
        "                                map(\n",
        "                                    lambda data: json.dumps(data),\n",
        "                                    offset_response.json()[\"results\"],\n",
        "                                )\n",
        "                            )\n",
        "                            all_data.extend(offset_data)\n",
        "\n",
        "                        else:\n",
        "                            print(\"error getting data\")\n",
        "                        counter += limit\n",
        "                        gc.collect\n",
        "                else:\n",
        "                    print(f\"Request failed with status code:{response.status_code}\")\n",
        "\n",
        "            except requests.exceptions.RequestException as e:\n",
        "                print(f\"Attemp {retry_count +1}/{max_retries} failed: {e}\")\n",
        "            else:\n",
        "                break\n",
        "            retry_count += 1\n",
        "\n",
        "        if retry_count == max_retries:\n",
        "            print(f\"Request failed 3 times. Failing task\")\n",
        "            if response is not None:\n",
        "                print(f\"Response content: {response.content}\")\n",
        "        else:\n",
        "            df = pd.DataFrame(all_data)\n",
        "            df.to_csv(\"provider_data.csv\")\n",
        "            print(df)\n",
        "\n",
        ""
      ]
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "K3sZ45ikdXnz"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}
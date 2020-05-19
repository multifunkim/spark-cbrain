FROM multifunkim/matlab-runtime:r2019b

LABEL maintainer="@aliobaibk" \
      version="1.0" \
      description="SPARK MATLAB standalone version"



# Environment
ENV LANG='C.UTF-8' \
    MFIL_HOME="/usr/local/Multi_FunkIm" \
    SPARK_VERSION="cbrain-mcv97"
ENV SPARK_DIR="$MFIL_HOME/spark_$SPARK_VERSION"



# Dependencies: MINC Toolkit and PyBIDS
RUN apt-get -y update && \
    apt-get -y --no-install-recommends install imagemagick perl python3-pip && \
    wget --no-check-certificate --quiet -O "/tmp/minc-toolkit.deb" \
    "https://packages.bic.mni.mcgill.ca/minc-toolkit/Debian/minc-toolkit-1.9.17-20190313-Ubuntu_18.04-x86_64.deb" && \
    dpkg -i "/tmp/minc-toolkit.deb" && \
    rm -f "/tmp/minc-toolkit.deb" && \
    echo '' >> /root/.bashrc && \
    echo '#minc-toolkit' >> /root/.bashrc && \
    echo ". '/opt/minc/1.9.17/minc-toolkit-config.sh'" >> /root/.bashrc && \
    pip3 install 'bids_validator==1.5.2' && \
    apt-get -y autoremove && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/* /root/.cache



# Install SPARK (binaries could be hosted in a server in the future for auto build)
COPY spark-bin.zip /tmp/
RUN mkdir -p "$MFIL_HOME/bin" "$SPARK_DIR" && \
    unzip -q /tmp/spark-bin.zip -d "$SPARK_DIR" && \
    rm -f /tmp/spark-bin.zip && \
    chmod +x -R "$SPARK_DIR" && \
    ln -s "$SPARK_DIR/spark.py" "$MFIL_HOME/bin/spark"



# Update environment
# minc-toolkit configuration parameters for 1.9.17-20190313
ENV MINC_TOOLKIT="/opt/minc/1.9.17" \
    MINC_TOOLKIT_VERSION="1.9.17-20190313"
ENV PATH="${MINC_TOOLKIT}/bin:${MINC_TOOLKIT}/pipeline:${PATH}" \
    PERL5LIB="${MINC_TOOLKIT}/perl:${MINC_TOOLKIT}/pipeline:${PERL5LIB}" \
    LD_LIBRARY_PATH="${MINC_TOOLKIT}/lib:${MINC_TOOLKIT}/lib/InsightToolkit:${LD_LIBRARY_PATH}" \
    MNI_DATAPATH="${MINC_TOOLKIT}/../share" \
    MINC_FORCE_V2=1 \
    MINC_COMPRESS=4 \
    VOLUME_CACHE_THRESHOLD=-1 \
    MANPATH="${MINC_TOOLKIT}/man:${MANPATH}"

# SPARK
ENV PATH="$MFIL_HOME/bin:$PATH"
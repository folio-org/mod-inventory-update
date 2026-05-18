<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output indent="yes" method="xml" version="1.0" encoding="UTF-8"/>
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
    <!-- Map locations -->
    <xsl:template match="permanentLocationId">
        <xsl:variable name="electronicholding" select="substring(//datafield[@tag='002@']/subfield[@code='0'],1,1)"/>
        <permanentLocationId>
            <xsl:choose>
                <!-- Online -->
                <xsl:when test="$electronicholding='O'">184aae84-a5bf-4c6a-85ba-4a7c73026cd5</xsl:when>
                <!-- Main Library -->
                <xsl:otherwise>fcd64ce1-6995-48f0-840e-89ffa2288371</xsl:otherwise>
            </xsl:choose>
        </permanentLocationId>
    </xsl:template>
</xsl:stylesheet>

<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="@*|node()">
    <xsl:choose>
      <xsl:when test="@code='a'[contains(., '@')]">
        <xsl:value-of select="concat(substring-before(@code='a', '@'), substring-after(@code='a', '@'))"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="."/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
</xsl:stylesheet>

<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
        version="1.0"
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:marc="http://www.loc.gov/MARC21/slim">


    <xsl:output indent="yes" method="xml" version="1.0" encoding="UTF-8"/>

    <xsl:template match="/">
        <collection>
            <xsl:apply-templates/>
        </collection>
    </xsl:template>

    <!-- MARC meta data -->
    <xsl:template match="marc:record">
        <record>
            <!-- Bibliographic record for FOLIO inventory -->
            <instance>
                <source>Sample</source>
                <hrid><xsl:value-of select="marc:controlfield[@tag='001']"/></hrid>

                <!-- title -->
                <title>
                    <xsl:variable name="concat-title">
                        <xsl:for-each
                                select="marc:datafield[@tag='245'][1]/marc:subfield[@code='a' or @code='b' or @code='h' or @code='n' or @code='p']">
                            <xsl:value-of select="."/>
                            <xsl:if test="position() != last()">
                                <xsl:text> </xsl:text>
                            </xsl:if>
                        </xsl:for-each>
                    </xsl:variable>
                    <xsl:call-template name="remove-characters-last">
                        <xsl:with-param name="input" select="$concat-title"/>
                        <xsl:with-param name="characters">,-./ :;</xsl:with-param>
                    </xsl:call-template>
                </title>

                <!-- Instance type ID (resource type) -->
                <instanceTypeId>
                    <!-- UUIDs for resource types -->
                    <xsl:choose>
                        <xsl:when test="substring(marc:leader,7,1)='a'">6312d172-f0cf-40f6-b27d-9fa8feaf332f</xsl:when>
                        <!-- language material : text -->
                        <xsl:when test="substring(marc:leader,7,1)='c'">497b5090-3da2-486c-b57f-de5bb3c2e26d</xsl:when>
                        <!-- notated music : notated music -->
                        <xsl:when test="substring(marc:leader,7,1)='d'">497b5090-3da2-486c-b57f-de5bb3c2e26d</xsl:when>
                        <!-- manuscript notated music : notated music -> notated music -->
                        <xsl:when test="substring(marc:leader,7,1)='e'">526aa04d-9289-4511-8866-349299592c18</xsl:when>
                        <!-- cartographic material : cartographic image -->
                        <xsl:when test="substring(marc:leader,7,1)='f'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f</xsl:when>
                        <!-- other --> <!-- manuscript cartographic material : ? -->
                        <xsl:when test="substring(marc:leader,7,1)='g'">535e3160-763a-42f9-b0c0-d8ed7df6e2a2</xsl:when>
                        <!-- projected image : still image -->
                        <xsl:when test="substring(marc:leader,7,1)='i'">9bce18bd-45bf-4949-8fa8-63163e4b7d7f</xsl:when>
                        <!-- nonmusical sound recording : sounds -->
                        <xsl:when test="substring(marc:leader,7,1)='j'">3be24c14-3551-4180-9292-26a786649c8b</xsl:when>
                        <!-- musical sound recording : performed music -->
                        <xsl:when test="substring(marc:leader,7,1)='k'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f</xsl:when>
                        <!-- other --> <!-- two-dimensional nonprojectable graphic : ?-->
                        <xsl:when test="substring(marc:leader,7,1)='m'">df5dddff-9c30-4507-8b82-119ff972d4d7</xsl:when>
                        <!-- computer file : computer dataset -->
                        <xsl:when test="substring(marc:leader,7,1)='o'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f</xsl:when>
                        <!-- kit : other -->
                        <xsl:when test="substring(marc:leader,7,1)='p'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f</xsl:when>
                        <!-- mixed material : other -->
                        <xsl:when test="substring(marc:leader,7,1)='r'">c1e95c2b-4efc-48cf-9e71-edb622cf0c22</xsl:when>
                        <!-- three-dimensional artifact or naturally occurring object : three-dimensional form -->
                        <xsl:when test="substring(marc:leader,7,1)='t'">6312d172-f0cf-40f6-b27d-9fa8feaf332f</xsl:when>
                        <!-- manuscript language material : text -->
                        <xsl:otherwise>a2c91e87-6bab-44d6-8adb-1fd02481fc4f</xsl:otherwise>
                        <!--  : other -->
                    </xsl:choose></instanceTypeId>

                <!-- Contributors -->
                <xsl:if test="marc:datafield[@tag='100' or @tag='110' or @tag='111' or @tag='700' or @tag='710' or @tag='711']">
                    <contributors>
                        <arr>
                            <xsl:for-each
                                    select="marc:datafield[@tag='100' or @tag='110' or @tag='111' or @tag='700' or @tag='710' or @tag='711']">
                                <i>
                                    <name>
                                        <xsl:for-each
                                                select="marc:subfield[@code='a' or @code='b' or @code='c' or @code='d' or @code='f' or @code='g' or @code='j' or @code='k' or @code='l' or @code='n' or @code='p' or @code='q' or @code='t' or @code='u']">
                                            <xsl:if test="position() > 1">
                                                <xsl:text>, </xsl:text>
                                            </xsl:if>
                                            <xsl:call-template name="remove-characters-last">
                                                <xsl:with-param name="input" select="."/>
                                                <xsl:with-param name="characters">,-.</xsl:with-param>
                                            </xsl:call-template>
                                        </xsl:for-each>
                                    </name>
                                    <xsl:choose>
                                        <xsl:when test="@tag='100' or @tag='700'">
                                            <contributorNameTypeId>2b94c631-fca9-4892-a730-03ee529ffe2a</contributorNameTypeId>
                                            <!-- personal name -->
                                            <xsl:if test="@tag='100'">
                                                <primary>true</primary>
                                            </xsl:if>
                                        </xsl:when>
                                        <xsl:when test="@tag='110' or @tag='710'">
                                            <contributorNameTypeId>2e48e713-17f3-4c13-a9f8-23845bb210aa</contributorNameTypeId>
                                            <!-- corporate name -->
                                        </xsl:when>
                                        <xsl:when test="@tag='111' or @tage='711'">
                                            <contributorNameTypeId>e8b311a6-3b21-43f2-a269-dd9310cb2d0a</contributorNameTypeId>
                                            <!-- meeting name -->
                                        </xsl:when>
                                        <xsl:otherwise>
                                            <contributorNameTypeId>2b94c631-fca9-4892-a730-03ee529ffe2a</contributorNameTypeId>
                                            <!-- personal name -->
                                        </xsl:otherwise>
                                    </xsl:choose>
                                    <xsl:if test="marc:subfield[@code='e' or @code='4']">
                                        <contributorTypeId>
                                        </contributorTypeId>
                                    </xsl:if>
                                </i>
                            </xsl:for-each>
                        </arr></contributors>
                </xsl:if>
            </instance>
            <original>
                <xsl:copy>
                    <xsl:copy-of select="@*"/>
                    <xsl:copy-of select="*"/>
                </xsl:copy>
            </original>
        </record>
    </xsl:template>

    <xsl:template match="text()"/>
    <xsl:template name="remove-characters-last">
        <xsl:param name="input"/>
        <xsl:param name="characters"/>
        <xsl:variable name="lastcharacter" select="substring($input,string-length($input))"/>
        <xsl:choose>
            <xsl:when test="$characters and $lastcharacter and contains($characters, $lastcharacter)">
                <xsl:call-template name="remove-characters-last">
                    <xsl:with-param name="input" select="substring($input,1, string-length($input)-1)"/>
                    <xsl:with-param name="characters" select="$characters"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$input"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:stylesheet>

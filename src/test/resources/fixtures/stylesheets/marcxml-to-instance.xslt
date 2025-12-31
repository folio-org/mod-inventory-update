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

      <!-- Information needed for storing source record in union catalog context -->
      <xsl:variable name="localId"
                    select="marc:datafield[@tag='999' and @ind1='f' and @ind2='f']/marc:subfield[@code='i']"/>
      <institutionIdHere/>
      <localIdentifier>
        <xsl:value-of select="$localId"/>
      </localIdentifier>

      <!-- Bibliographic record for FOLIO inventory -->
      <instance>
        <source>MARC</source>
        <hrid><xsl:value-of select="marc:controlfield[@tag='001']"/></hrid>
        <!-- Instance type ID (resource type) -->
        <instanceTypeId>
          <!-- UUIDs for resource types -->
          <xsl:choose>
            <xsl:when test="substring(marc:leader,7,1)='a'">6312d172-f0cf-40f6-b27d-9fa8feaf332f
            </xsl:when> <!-- language material : text -->
            <xsl:when test="substring(marc:leader,7,1)='c'">497b5090-3da2-486c-b57f-de5bb3c2e26d
            </xsl:when> <!-- notated music : notated music -->
            <xsl:when test="substring(marc:leader,7,1)='d'">497b5090-3da2-486c-b57f-de5bb3c2e26d
            </xsl:when> <!-- manuscript notated music : notated music -> notated music -->
            <xsl:when test="substring(marc:leader,7,1)='e'">526aa04d-9289-4511-8866-349299592c18
            </xsl:when> <!-- cartographic material : cartographic image -->
            <xsl:when test="substring(marc:leader,7,1)='f'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f
            </xsl:when> <!-- other --> <!-- manuscript cartographic material : ? -->
            <xsl:when test="substring(marc:leader,7,1)='g'">535e3160-763a-42f9-b0c0-d8ed7df6e2a2
            </xsl:when> <!-- projected image : still image -->
            <xsl:when test="substring(marc:leader,7,1)='i'">9bce18bd-45bf-4949-8fa8-63163e4b7d7f
            </xsl:when> <!-- nonmusical sound recording : sounds -->
            <xsl:when test="substring(marc:leader,7,1)='j'">3be24c14-3551-4180-9292-26a786649c8b
            </xsl:when> <!-- musical sound recording : performed music -->
            <xsl:when test="substring(marc:leader,7,1)='k'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f
            </xsl:when> <!-- other --> <!-- two-dimensional nonprojectable graphic : ?-->
            <xsl:when test="substring(marc:leader,7,1)='m'">df5dddff-9c30-4507-8b82-119ff972d4d7
            </xsl:when> <!-- computer file : computer dataset -->
            <xsl:when test="substring(marc:leader,7,1)='o'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f
            </xsl:when> <!-- kit : other -->
            <xsl:when test="substring(marc:leader,7,1)='p'">a2c91e87-6bab-44d6-8adb-1fd02481fc4f
            </xsl:when> <!-- mixed material : other -->
            <xsl:when test="substring(marc:leader,7,1)='r'">c1e95c2b-4efc-48cf-9e71-edb622cf0c22
            </xsl:when> <!-- three-dimensional artifact or naturally occurring object : three-dimensional form -->
            <xsl:when test="substring(marc:leader,7,1)='t'">6312d172-f0cf-40f6-b27d-9fa8feaf332f
            </xsl:when> <!-- manuscript language material : text -->
            <xsl:otherwise>a2c91e87-6bab-44d6-8adb-1fd02481fc4f
            </xsl:otherwise>                             <!--  : other -->
          </xsl:choose>
        </instanceTypeId>

        <!-- Identifiers -->
        <xsl:if test="marc:datafield[@tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='074']
      or marc:controlfield[@tag='001']">
          <identifiers>
            <arr>
              <i>
                <value>
                  <xsl:value-of select="$localId"/>
                </value>
                <!-- A subsequent library specific transformation (style sheet)
                must replace this tag with the actual identifierTypeId for
                the record identifier type of the given library -->
                <identifierTypeIdHere/>
              </i>
              <xsl:for-each
                  select="marc:datafield[@tag='001' or @tag='010' or @tag='020' or @tag='022' or @tag='024' or @tag='028' or @tag='035' or @tag='074']">
                <i>
                  <xsl:choose>
                    <xsl:when test="current()[@tag='010'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>c858e4f2-2b6b-4385-842b-60732ee14abb</identifierTypeId> <!-- LCCN -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='020'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>8261054f-be78-422d-bd51-4ed9f33c3422</identifierTypeId> <!-- ISBN -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='022'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>913300b2-03ed-469a-8179-c1092c991227</identifierTypeId> <!-- ISSN -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='024'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>2e8b3b6c-0e7d-4e48-bca2-b0b23b376af5
                      </identifierTypeId> <!-- Other standard identifier -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='028'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>b5d8cdc4-9441-487c-90cf-0c7ec97728eb
                      </identifierTypeId> <!-- Publisher number -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='035'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>7e591197-f335-4afb-bc6d-a6d76ca3bace
                      </identifierTypeId> <!-- System control number -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='074'] and marc:subfield[@code='a']">
                      <value>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </value>
                      <identifierTypeId>351ebc1c-3aae-4825-8765-c6d50dbf011f</identifierTypeId> <!-- GPO item number -->
                    </xsl:when>
                  </xsl:choose>
                </i>
              </xsl:for-each>
            </arr>
          </identifiers>
        </xsl:if>

        <!-- Classifications -->
        <xsl:if test="marc:datafield[@tag='050' or @tag='060' or @tag='080' or @tag='082' or @tag='086' or @tag='090']">
          <classifications>
            <arr>
              <xsl:for-each
                  select="marc:datafield[@tag='050' or @tag='060' or @tag='080' or @tag='082' or @tag='086' or @tag='090']">
                <i>
                  <xsl:choose>
                    <xsl:when test="current()[@tag='050']">
                      <classificationNumber>
                        <xsl:for-each select="marc:subfield[@code='a' or @code='b']">
                          <xsl:if test="position() > 1">
                            <xsl:text>; </xsl:text>
                          </xsl:if>
                          <xsl:value-of select="."/>
                        </xsl:for-each>
                      </classificationNumber>
                      <classificationTypeId>ce176ace-a53e-4b4d-aa89-725ed7b2edac
                      </classificationTypeId> <!-- LC, Library of Congress -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='082']">
                      <classificationNumber>
                        <xsl:for-each select="marc:subfield[@code='a' or @code='b']">
                          <xsl:if test="position() > 1">
                            <xsl:text>; </xsl:text>
                          </xsl:if>
                          <xsl:value-of select="."/>
                        </xsl:for-each>
                      </classificationNumber>
                      <classificationTypeId>42471af9-7d25-4f3a-bf78-60d29dcf463b</classificationTypeId> <!-- Dewey -->
                    </xsl:when>
                    <xsl:when test="current()[@tag='086']">
                      <classificationNumber>
                        <xsl:value-of select="marc:subfield[@code='a']"/>
                      </classificationNumber>
                      <classificationTypeId>9075b5f8-7d97-49e1-a431-73fdd468d476</classificationTypeId> <!-- SUDOC -->
                    </xsl:when>
                  </xsl:choose>
                </i>
              </xsl:for-each>
            </arr>
          </classifications>
        </xsl:if>

        <!-- title -->
        <title>
          <xsl:variable name="dirty-title">
            <xsl:for-each
                select="marc:datafield[@tag='245'][1]/marc:subfield[@code='a' or @code='b' or @code='h' or @code='n' or @code='p']">
              <xsl:value-of select="."/>
              <xsl:if test="position() != last()">
                <xsl:text> </xsl:text>
              </xsl:if>
            </xsl:for-each>
          </xsl:variable>
          <xsl:call-template name="remove-characters-last">
            <xsl:with-param name="input" select="$dirty-title"/>
            <xsl:with-param name="characters">,-./ :;</xsl:with-param>
          </xsl:call-template>
        </title>

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
                      <contributorNameTypeId>2b94c631-fca9-4892-a730-03ee529ffe2a
                      </contributorNameTypeId> <!-- personal name -->
                      <xsl:if test="@tag='100'">
                        <primary>true</primary>
                      </xsl:if>
                    </xsl:when>
                    <xsl:when test="@tag='110' or @tag='710'">
                      <contributorNameTypeId>2e48e713-17f3-4c13-a9f8-23845bb210aa
                      </contributorNameTypeId> <!-- corporate name -->
                    </xsl:when>
                    <xsl:when test="@tag='111' or @tage='711'">
                      <contributorNameTypeId>e8b311a6-3b21-43f2-a269-dd9310cb2d0a
                      </contributorNameTypeId> <!-- meeting name -->
                    </xsl:when>
                    <xsl:otherwise>
                      <contributorNameTypeId>2b94c631-fca9-4892-a730-03ee529ffe2a
                      </contributorNameTypeId> <!-- personal name -->
                    </xsl:otherwise>
                  </xsl:choose>
                  <xsl:if test="marc:subfield[@code='e' or @code='4']">
                    <contributorTypeId>
                    </contributorTypeId>
                  </xsl:if>
                </i>
              </xsl:for-each>
            </arr>
          </contributors>
        </xsl:if>

        <!-- Editions -->
        <xsl:if test="marc:datafield[@tag='250']">
          <editions>
            <arr>
              <xsl:for-each select="marc:datafield[@tag='250']">
                <i>
                  <xsl:value-of select="marc:subfield[@code='a']"/>
                  <xsl:if test="marc:subfield[@code='b']">;
                    <xsl:value-of select="marc:subfield[@code='b']"/>
                  </xsl:if>
                </i>
              </xsl:for-each>
            </arr>
          </editions>
        </xsl:if>

        <!-- Publication -->
        <xsl:choose>
          <xsl:when test="marc:datafield[@tag='260' or @tag='264']">
            <publication>
              <arr>
                <xsl:for-each select="marc:datafield[@tag='260' or @tag='264']">
                  <i>
                    <publisher>
                      <xsl:value-of select="marc:subfield[@code='b']"/>
                    </publisher>
                    <place>
                      <xsl:value-of select="marc:subfield[@code='a']"/>
                    </place>
                    <dateOfPublication>
                      <xsl:value-of select="marc:subfield[@code='c']"/>
                    </dateOfPublication>
                  </i>
                </xsl:for-each>
              </arr>
            </publication>
          </xsl:when>
          <xsl:otherwise>
            <publication>
              <arr>
                <i>
                  <dateOfPublication>
                    <xsl:value-of select="substring(marc:controlfield[@tag='008'],8,4)"/>
                  </dateOfPublication>
                </i>
              </arr>
            </publication>
          </xsl:otherwise>
        </xsl:choose>

        <!-- physicalDescriptions -->
        <xsl:if test="marc:datafield[@tag='300']">
          <physicalDescriptions>
            <arr>
              <xsl:for-each select="marc:datafield[@tag='300']">
                <i>
                  <xsl:call-template name="remove-characters-last">
                    <xsl:with-param name="input" select="marc:subfield[@code='a']"/>
                    <xsl:with-param name="characters">,-./ :;</xsl:with-param>
                  </xsl:call-template>
                </i>
              </xsl:for-each>
            </arr>
          </physicalDescriptions>
        </xsl:if>

        <!-- Subjects -->
        <xsl:if
            test="marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='648' or @tag='650' or @tag='651' or @tag='653' or @tag='654' or @tag='655' or @tag='656' or @tag='657' or @tag='658' or @tag='662' or @tag='69X']">
          <subjects>
            <arr>
              <xsl:for-each
                  select="marc:datafield[@tag='600' or @tag='610' or @tag='611' or @tag='630' or @tag='648' or @tag='650' or @tag='651' or @tag='653' or @tag='654' or @tag='655' or @tag='656' or @tag='657' or @tag='658' or @tag='662' or @tag='69X']">
                <i>
                  <xsl:for-each
                      select="marc:subfield[@code='a' or @code='b' or @code='c' or @code='d' or @code='f' or @code='g' or @code='j' or @code='k' or @code='l' or @code='n' or @code='p' or @code='q' or @code='t' or @code='u' or @code='v' or @code='x' or @code='y' or @code='z']">
                    <xsl:if test="position() > 1">
                      <xsl:text>--</xsl:text>
                    </xsl:if>
                    <xsl:call-template name="remove-characters-last">
                      <xsl:with-param name="input" select="."/>
                      <xsl:with-param name="characters">,-.</xsl:with-param>
                    </xsl:call-template>
                  </xsl:for-each>
                </i>
              </xsl:for-each>
            </arr>
          </subjects>
        </xsl:if>

        <!-- Notes -->
        <xsl:if test="marc:datafield[@tag='500' or @tag='504' or @tag='505' or @tag='520']">
          <notes>
            <arr>
              <xsl:for-each select="marc:datafield[@tag='500' or @tag='504' or @tag='505' or @tag='520']">
                <i>
                  <note>
                    <xsl:value-of select="normalize-space(.)"/>
                  </note>
                  <instanceNoteTypeId>
                    <xsl:choose>
                      <xsl:when test='./@tag="504"'>86b6e817-e1bc-42fb-bab0-70e7547de6c1</xsl:when> <!-- biliography -->
                      <xsl:when test='./@tag="505"'>5ba8e385-0e27-462e-a571-ffa1fa34ea54</xsl:when> <!-- contents -->
                      <xsl:when test='./@tag="520"'>0e2e11b-450f-45c8-b09b-0f819999966e</xsl:when> <!-- contents -->
                      <xsl:otherwise>6a2533a7-4de2-4e64-8466-074c2fa9308c</xsl:otherwise> <!-- general -->
                    </xsl:choose>
                  </instanceNoteTypeId>
                </i>
              </xsl:for-each>
            </arr>
          </notes>
        </xsl:if>
      </instance>

      <!-- Additional info for creating match key in FOLIO Inventory match module -->
      <matchKey>
        <xsl:for-each select="marc:datafield[@tag='245']">
          <title>
            <xsl:call-template name="remove-characters-last">
              <xsl:with-param name="input" select="marc:subfield[@code='a']"/>
              <xsl:with-param name="characters">,-./ :;</xsl:with-param>
            </xsl:call-template>
          </title>
          <remainder-of-title>
            <xsl:text> : </xsl:text>
            <xsl:call-template name="remove-characters-last">
              <xsl:with-param name="input" select="marc:subfield[@code='b']"/>
              <xsl:with-param name="characters">,-./ :;</xsl:with-param>
            </xsl:call-template>
          </remainder-of-title>
          <medium>
            <xsl:call-template name="remove-characters-last">
              <xsl:with-param name="input" select="marc:subfield[@code='h']"/>
              <xsl:with-param name="characters">,-./ :;</xsl:with-param>
            </xsl:call-template>
          </medium>
          <name-of-part-section-of-work>
            <xsl:value-of select="marc:subfield[@code='p']"/>
          </name-of-part-section-of-work>
          <number-of-part-section-of-work>
            <xsl:value-of select="marc:subfield[@code='n']"/>
          </number-of-part-section-of-work>
          <inclusive-dates>
            <xsl:value-of select="marc:subfield[@code='f']"/>
          </inclusive-dates>
        </xsl:for-each>
      </matchKey>

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
